from datetime import timedelta
import boto3
import os
from io import StringIO, BytesIO
import feedparser
import pandas as pd
import spacy
import requests
import pybase64
from google.cloud import bigquery, storage
import shutil 

from glob import glob
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
# from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
# from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from datetime import datetime

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from utils import upload_formatted_rss_feed,scrape_reviews,scrape_appdetails,get_unique_appids,analyze_sentiment,label_polarity,upload_string_to_gcs,webhook_message

BUCKET_NAME = "news_sites"
DATE_NOW = datetime.now().strftime("%Y-%m-%d")
# Data directory for CSVs and OSM Images
DATA_PATH = '/opt/airflow/data/'

GAME_ARTICLE_FILENAME = f"{DATA_PATH}game_articles_{DATE_NOW}.csv"
GAME_REVIEWS_FILENAME = f"{DATA_PATH}game_reviews_{DATE_NOW}.csv"
GAME_DETAILS_FILENAME = f"{DATA_PATH}game_details_{DATE_NOW}.csv"

##################################################################################################################################
################################################# 1.) EXTRACT #################################################################
##################################################################################################################################
@task(task_id="kotaku")
def kotaku_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("kotaku","https://kotaku.com/rss")

@task(task_id="escapist_mag")
def escapist_mag_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("escapist_mag","https://www.escapistmagazine.com/v2/feed/")

@task(task_id="eurogamer")
def eurogamer_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("eurogamer","https://www.eurogamer.net/?format=rss")



@task(task_id="indigames_plus")
def indigames_plus_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("indigames_plus","https://indiegamesplus.com/feed")

@task(task_id="rock_paper_sg")
def rock_paper_sg_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("rock_paper_sg","http://feeds.feedburner.com/RockPaperShotgun")


@task(task_id="ancient_gaming")
def ancient_gaming_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("ancient_gaming","http://feeds.feedburner.com/TheAncientGamingNoob")


@task(task_id="combine_all_articles")
def combine_all_articles(ds=None,**kwargs):
    files = os.listdir(f"{DATA_PATH}articles")
    dfs = []
    for file in files:
        outfile = f"{DATA_PATH}articles/{file}"
        if not outfile.endswith('.csv'):
            continue
        df = pd.read_csv(outfile,on_bad_lines="skip")
        dfs.append(df)
    game_articles = pd.concat(dfs)
    game_articles = game_articles.drop_duplicates()
    game_articles = game_articles.dropna(subset=["appids"])
    game_articles.to_csv(GAME_ARTICLE_FILENAME,index=False)

@task(task_id="scrape_game_reviews")
def scrape_game_reviews(ds=None,**kwargs):
    game_articles = pd.read_csv(GAME_ARTICLE_FILENAME,on_bad_lines="skip")
    appids = get_unique_appids(game_articles)

    all_reviews = []
    for appid in appids:
        reviews_list = scrape_reviews(appid)
        all_reviews += reviews_list
    game_reviews = pd.DataFrame(all_reviews)

    game_reviews = game_reviews.drop_duplicates(subset=["review"])

    game_reviews["timestamp_created"] = game_reviews["timestamp_created"].apply(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))
    game_reviews["timestamp_updated"] = game_reviews["timestamp_updated"].apply(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))
    game_reviews.to_csv(GAME_REVIEWS_FILENAME,index=False)

@task(task_id="scrape_game_details")
def scrape_game_details(ds=None,**kwargs):
    game_articles = pd.read_csv(GAME_ARTICLE_FILENAME,on_bad_lines="skip")
    appids = get_unique_appids(game_articles)
    
    lst_game_details = scrape_appdetails(appids)
    
    game_details = pd.DataFrame(lst_game_details).rename(columns={"appid":"appids"})
    game_details = game_details.drop_duplicates(subset=["appids"])
    game_details.to_csv(GAME_DETAILS_FILENAME,index=False)


##################################################################################################################################
################################################# 2.) TRANSFORM  #################################################################
##################################################################################################################################


@task(task_id="word_count")
def word_count(ds=None, **kwargs):

    def word_count(text):
        words = text.split()
        freq = [words.count(w) for w in words]
        word_dict = dict(zip(words, freq))
        return word_dict



    game_articles = pd.read_csv(GAME_ARTICLE_FILENAME,on_bad_lines="skip")
    game_reviews = pd.read_csv(GAME_REVIEWS_FILENAME,on_bad_lines="skip")

#############################################################################################
    print(f"getting word_count for game articles...")
    game_articles['sum_word_cnt'] = game_articles['summary'].apply(lambda x: len(str(x).split()))
    game_articles['dict_word_cnt'] = game_articles['summary'].apply(lambda x: word_count(str(x)))
    game_articles.to_csv(GAME_ARTICLE_FILENAME, index=False)
    print(f"word_count for game_articles collected!")
    print()

#############################################################################################
    print(f"getting word_count for game reviews...")
    game_reviews['sum_word_cnt'] = game_reviews['review'].apply(lambda x: len(str(x).split()))
    game_reviews['dict_word_cnt'] = game_reviews['review'].apply(lambda x: word_count(str(x)))

    game_reviews.to_csv(GAME_REVIEWS_FILENAME,index=False)
    print(f"word_count for game reviews collected!")
    print()

@task(task_id='sentiment_analysis')
def sentiment_analysis(ds=None,**kwargs):

    game_articles = pd.read_csv(GAME_ARTICLE_FILENAME,on_bad_lines="skip")
    game_reviews = pd.read_csv(GAME_REVIEWS_FILENAME,on_bad_lines="skip")

    print("getting sentiment for articles...")
    game_articles['polarity_score_summary'] = game_articles['summary'].apply(lambda x: analyze_sentiment(str(x)))
    game_articles["sentiment_summary"] = game_articles["polarity_score_summary"].apply(lambda x: label_polarity(x["compound"]))
        
    game_articles['polarity_score_title'] = game_articles['title'].apply(lambda x: analyze_sentiment(str(x)))
    game_articles["sentiment_title"] = game_articles["polarity_score_title"].apply(lambda x: label_polarity(x["compound"]))
    game_articles.to_csv(GAME_ARTICLE_FILENAME, index=False)
    print("sentiment for articles taken!")

    #############################################################################################
    print("getting sentiment for game reviews...")
    game_reviews["sentiment"] = game_reviews["voted_up"].apply(lambda x: "positive" if x else "negative")
    game_reviews.to_csv(GAME_REVIEWS_FILENAME,index=False)
    print("sentiment for reviews taken!")
# NER
@task(task_id='spacy_ner')
def spacy_ner(ds=None, **kwargs):
    nlp = spacy.load("/model/en_core_web_sm/en_core_web_sm-3.3.0")
    
    def ner(text):
        doc = nlp(text)
        ner = {}
        for entity in doc.ents:
            ner[entity.text] = entity.label_
            print(entity.text, entity.label_)
        return ner

    game_articles = pd.read_csv(GAME_ARTICLE_FILENAME,on_bad_lines="skip")
    game_reviews = pd.read_csv(GAME_REVIEWS_FILENAME,on_bad_lines="skip")

    print("getting NER for game articles...")
    game_articles['NER'] = game_articles['summary'].apply(lambda x: ner(str(x)))
    game_articles.to_csv(GAME_ARTICLE_FILENAME,index=False)
    print("getting NER for game articles complete!")
    print()
    ###########################################################################################
    
    print("getting NER for game reviews...")
    game_reviews["NER"] = game_reviews["review"].apply(lambda x: ner(str(x)))
    game_reviews.to_csv(GAME_REVIEWS_FILENAME,index=False)
    print("getting NER for game reviews complete!")
    print()

##################################################################################################################################
################################################# 3.) LOAD  ######################################################################
##################################################################################################################################


@task(task_id="load_data")
def load_data(ds=None, **kwargs):
    files = os.listdir(DATA_PATH)
    for file in files:
        outfile = f"{DATA_PATH}{file}"
        if not outfile.endswith('.csv'):
            continue
        df = pd.read_csv(outfile,on_bad_lines="skip")
        csv_buffer = StringIO()

        if "details" in file:
            filename = f"game_details/{file}"
        elif "articles" in file:
            filename = f"game_articles/{file}"
        else:
            filename = f"game_reviews/{file}"

        df.to_csv(csv_buffer,index=False)
        upload_string_to_gcs(csv_body=csv_buffer, uploaded_filename=filename)


@task(task_id='delete_residuals')
def delete_residuals(ds=None, **kwargs): 
    files = os.listdir(f"{DATA_PATH}")
    for file in files: 
        outfile = f"{DATA_PATH}{file}"
        print(file)
        if os.path.isdir(outfile):
            shutil.rmtree(outfile)
        else: 
            os.remove(outfile)

@task(task_id="data_validation")
def data_validation(ds=None,**kwargs):
    game_articles = pd.read_csv(GAME_ARTICLE_FILENAME)
    game_reviews = pd.read_csv(GAME_REVIEWS_FILENAME)
    game_details = pd.read_csv(GAME_DETAILS_FILENAME)

    game_details = game_details.dropna(subset=["name"])
    game_details = game_details.rename(columns={"appid":"appids"})
    game_details = game_details.drop_duplicates(subset=["appids"])
    game_details.to_csv(GAME_DETAILS_FILENAME,index=False)


    game_reviews = game_reviews.dropna(subset=["review"])
    game_reviews = game_reviews.drop_duplicates(subset=["review"])
    game_reviews.to_csv(GAME_REVIEWS_FILENAME,index=False)

    game_articles = game_articles.dropna(subset=["summary"])
    game_articles = game_articles.drop_duplicates()
    game_articles.to_csv(GAME_ARTICLE_FILENAME,index=False)



##################################################################################################################################
################################################# DAG ############################################################################
##################################################################################################################################




with DAG(
    'scrapers_proj_test',
    default_args={

    },
    description='Pipeline demo',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 25),
    catchup=False,
    tags=['scrapers'],
) as dag:


    dagstart_msg = BashOperator(
        task_id="dagstart_msg",
        bash_command = webhook_message("Dag tasks starting!"),
        dag=dag
    )

    article_scraping1 = BashOperator(
        task_id="article_scraping1",
        bash_command = webhook_message("task 1.0, initiating data extraction: article scraping 1!"),
        dag=dag
    )

    article_scraping1_end = BashOperator(
        task_id="article_scraping1_end",
        bash_command = webhook_message("continuing task 1.0: article scraping 1 complete!"),
        dag=dag
    )

    article_scraping2 = BashOperator(
        task_id="article_scraping2",
        bash_command = webhook_message("continuing task 1.0: article scraping 2!"),
        dag=dag
    )

    article_scraping2_end = BashOperator(
        task_id="article_scraping2_end",
        bash_command = webhook_message("ending task 1.0: article scraping complete!"),
        dag=dag
    )

    steam_scraping = BashOperator(
        task_id="steam_scraping",
        bash_command = webhook_message("starting task 1.5: scraping user reviews and game details!"),
        dag=dag
    )

    steam_scraping_end = BashOperator(
        task_id="data_extraction_end",
        bash_command = webhook_message("ending task 1.5: steam scraping complete! all data extracted."),
        dag=dag
    )

    transformation = BashOperator(
        task_id="transformation_start",
        bash_command = webhook_message("starting task 2.0: beginning data transformation"),
        dag=dag
    )
    transformation_end = BashOperator(
        task_id="transformation_end",
        bash_command = webhook_message("ending task 2.0: data transformation completed!"),
        dag=dag
    )

    loading_data = BashOperator(
        task_id="data_loading",
        bash_command = webhook_message("starting task 3.0: beginning data loading"),
        dag=dag
    )
    loading_data_end = BashOperator(
        task_id="data_loading_end",
        bash_command = webhook_message("ending task 3.0: data successfully loaded to the cloud!"),
        dag=dag
    )
    dagend_msg = BashOperator(
        task_id="dagend_msg",
        bash_command = webhook_message("All dag tasks completed!"),
        dag=dag
    )


    dagstart_msg \
    >> article_scraping1 >> [indigames_plus_feed(),kotaku_feed(), escapist_mag_feed()] >> article_scraping1_end \
    >> article_scraping2 >> [eurogamer_feed(),rock_paper_sg_feed(),ancient_gaming_feed()] >> combine_all_articles() >> article_scraping2_end \
    >> steam_scraping >> [scrape_game_details(),scrape_game_reviews()] >> steam_scraping_end \
    >> transformation >> sentiment_analysis() >> word_count() >> spacy_ner() >> transformation_end >> data_validation() \
    >> loading_data >> load_data() >> loading_data_end >> delete_residuals() >> \
    dagend_msg