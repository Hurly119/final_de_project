import requests
import feedparser

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

import os
from glob import glob

import requests
from bs4 import BeautifulSoup
from dateutil import parser

from datetime import datetime
from math import isnan

import spacy
from airflow.models import Variable

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import time
import random

import boto3


def upload_string_to_gcs(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    BUCKET_NAME = "final-de-storage"
    MY_FOLDER_PREFIX = "final_project"

    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    gcs_resource.Object(BUCKET_NAME, MY_FOLDER_PREFIX + "/" + uploaded_filename).put(Body=csv_body.getvalue())


def get_request(url,params=None,retries=0):
    sleep_time = random.randint(1,10)
    if retries >3:
        print("Failed to get response cancelling...,")
        return None
    try:
        response = requests.get(url=url,params=params,timeout=5)
    except Exception as e:
        retries +=1
        return get_request(url,params,retries)
        
    if response:
        return response.json()
    else:
        print("No Response. Retrying...")
        time.sleep(sleep_time)
        retries+=1
        return get_request(url,params,retries)

##initialize dfs
def get_appid(game_name):
    game_name = game_name.lower()
    response = requests.get(url=f'https://store.steampowered.com/search/?term={game_name}&category1=998', headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(response.text, 'html.parser')
    app_details = None
    app_id = None
    game_found = None
    try:
        app_details = soup.find(class_='search_result_row')
        app_id = app_details['data-ds-appid']
        game_found = app_details.find(class_="search_name").text.strip().lower()
    except:
        return None
    
    if game_name == game_found:
        return app_id
    return None

def date_published_tostr(published):
    return parser.parse(published).strftime("%Y-%m-%d %H:%M:%S")

def init_appids(df):
    df2 = df.copy()
    tags = df2["tags"].values
    source = df2["source"].iloc[0]
    appids = []
    for i,tag in enumerate(tags):
        try:
            if tag is None or isnan(tag):
                appids.append(None)
                print(f"{source}: game not found for:",i)
                continue
        except:
            pass
        ls_tag = None
        try:
            ls_tag = eval(tag)
        except:
            ls_tag = tag

        for term in ls_tag:
            game_name = term["term"]
            appid = get_appid(game_name)
            if appid:
                appids.append(appid)
                print(f"{source}: YAY",appid,game_name)
                break
        else:
            appids.append(None)
            print(f"{source}: game not found for:",i)
    return appids

def init_null_appids(nouns):
    for noun in nouns:
        appid = get_appid(noun)
        print(f"checking if game \'{noun}\' on steam...")
        if appid:
            print(f"game \'{noun}\' found!.")
            print()
            return appid
    print(f"article has no games on steam.")
    print()
    return None



def get_nouns(title_summary):
    nlp = spacy.load("/model/en_core_web_sm/en_core_web_sm-3.3.0")
    stop_words = stopwords.words("english")
    
    summary = BeautifulSoup(title_summary,"html.parser").text.strip().lower()
    doc = nlp(summary)
    noun_phrases = set([noun.text for noun in doc.noun_chunks])
    return [noun.strip() for noun in noun_phrases if noun not in stop_words]

def upload_formatted_rss_feed(feed_name, feed):
    DATA_PATH = '/opt/airflow/data/'
    if not os.path.exists(f"{DATA_PATH}articles"):
        os.mkdir(f"{DATA_PATH}articles")
    feed = feedparser.parse(feed)
    df = pd.DataFrame(feed['entries'])
    columns = ["title","title_detail","link","links","author","authors","published","published_parsed","tags","id","guidislink","summary"]
    df = df[columns]
    df["source"] = feed_name
    df = df_appids(df,feed_name)
    time_now = datetime.now().strftime('%Y-%m-%d')
    filename = feed_name + '_' + time_now + '.csv'
    df.to_csv(f"{DATA_PATH}articles/{filename}", index=False)
    print(f"df {filename} saved to .csv!")

def df_appids(df2,feed_name):
    df = df2.copy()

    appids = init_appids(df)
    print(f"getting appids for {feed_name}...")
    df["appids"] = appids
    df["summary"] = df["summary"].apply(lambda x: BeautifulSoup(x,"html.parser").text.strip().lower())
    df["nouns"] = df["title"].apply(get_nouns)
    df["date_str"] = df["published"].apply(date_published_tostr)
    df.loc[df["appids"].isnull(),"appids"]= df.loc[df["appids"].isnull(),"nouns"].apply(init_null_appids)
    print(f"appids for {feed_name} initialized!")
    return df

def reviews_json_to_df(review_response,appid):
    all_reviews = []
    print(review_response)
    for review in review_response["reviews"]:
        author_data = review.pop("author")
        review.update(author_data)
        review.update({"appids":appid})
        all_reviews.append(review)
    return all_reviews

def scrape_reviews(appid):
    params = {'json' : 1,
        'filter' : 'all',
        'language' : 'english',
        'review_type' : 'all',
        'purchase_type' : 'all',
        'day_range':1}

    cursor = False
    explored_cursors = []
    reviews_list = []

    while cursor not in explored_cursors:
        explored_cursors.append(cursor)
        have_cursor = True
        reviews = get_request(f"https://store.steampowered.com/appreviews/{appid}",params=params)
        try:
            cursor = reviews["cursor"]

            params["cursor"] = cursor.encode()
        except:
            have_cursor = False

        params["num_per_page"] = 100
        print("review count:",len(reviews_list))
        print()
        reviews_list += reviews_json_to_df(reviews,appid)

        if not have_cursor: break
    return reviews_list

# def scrape_appdetails(appids):
def scrape_appdetails(appids):
    list_appdetails = []
    for appid in appids:
        print(f"getting appdetails for: {appid}")
        app_detail = get_request(f"https://steamspy.com/api.php?request=appdetails&appid={appid}")
        app_detail["tags"] = [app_detail["tags"]]
        list_appdetails.append(app_detail)
    return pd.DataFrame(list_appdetails)


def get_unique_appids(game_articles):
    non_null_appids = game_articles[game_articles["appids"].notnull()]
    appids = non_null_appids["appids"].unique()
    return appids

def analyze_sentiment(sentence):
    sid_obj = SentimentIntensityAnalyzer()
    return sid_obj.polarity_scores(sentence)

def label_polarity(polarity):
    if polarity>=0.05:
        return "positive"
    elif polarity <=-0.05:
        return "negative"
    else:
        return "neutral"