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

from utils import upload_formatted_rss_feed,scrape_reviews,scrape_appdetails,get_unique_appids,analyze_sentiment,label_polarity,upload_string_to_gcs

BUCKET_NAME = "news_sites"

# Group directory in the bucket
MY_FOLDER_PREFIX = "fem_hans"

DATE_NOW = datetime.now().strftime("%Y-%m-%d")
# Data directory for CSVs and OSM Images
DATA_PATH = '/opt/airflow/data/'

#################################################################
################### 1.) EXTRACT #################################
#################################################################
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
        df = pd.read_csv(outfile)
        dfs.append(df)
    game_articles = pd.concat(dfs)
    game_articles = game_articles.drop_duplicates()
    game_articles.to_csv(f"{DATA_PATH}game_articles_{DATE_NOW}.csv")

@task(task_id="scrape_game_reviews")
def scrape_game_reviews(ds=None,**kwargs):
    game_articles = pd.read_csv(f"{DATA_PATH}game_articles_{DATE_NOW}.csv")
    appids = get_unique_appids(game_articles)

    all_reviews = []
    for appid in appids:
        reviews_list = scrape_reviews(appid)
        all_reviews += reviews_list
    game_reviews = pd.DataFrame(all_reviews)

    game_reviews = game_reviews.drop_duplicates(subset="review")

    game_reviews["timestamp_created"] = game_reviews["timestamp_created"].apply(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))
    game_reviews["timestamp_updated"] = game_reviews["timestamp_updated"].apply(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))
    game_reviews.to_csv(f"{DATA_PATH}game_reviews_{DATE_NOW}.csv")

@task(task_id="scrape_game_details")
def scrape_game_details(ds=None,**kwargs):
    game_articles = pd.read_csv(f"{DATA_PATH}game_articles_{DATE_NOW}.csv")
    appids = get_unique_appids(game_articles)
    
    lst_game_details = scrape_appdetails(appids)
    
    game_details = pd.DataFrame(lst_game_details).rename(columns={"appid":"appids"})
    game_details = game_details.drop_duplicates(subset="appids")
    game_details.to_csv(f"{DATA_PATH}game_details_{DATE_NOW}.csv")

#################################################################
################### 2.) TRANSFORM ###############################
#################################################################

@task(task_id="word_count")
def word_count(ds=None, **kwargs):

    def word_count(text):
        words = text.split()
        freq = [words.count(w) for w in words]
        word_dict = dict(zip(words, freq))
        return word_dict


    game_article_filename = f"{DATA_PATH}game_articles_{DATE_NOW}.csv"
    game_reviews_filename = f"{DATA_PATH}game_reviews_{DATE_NOW}.csv"

    game_articles = pd.read_csv(game_article_filename)
    game_reviews = pd.read_csv(game_reviews_filename)
        ################################# TODO: IMPORTANT #########################################
        # you need to find the column where the text/content is located e.g. 'summary' or 'content'
        # and add a conditional logic below
        ###########################################################################################
    print(f"getting word_count for game articles...")
    game_articles['sum_word_cnt'] = game_articles['summary'].apply(lambda x: len(str(x).split()))
    game_articles['dict_word_cnt'] = game_articles['summary'].apply(lambda x: word_count(str(x)))
        ###########################################################################################

    game_articles.to_csv(game_article_filename, index=False)
    print(f"word_count for game_articles collected!")
    print()

    print(f"getting word_count for game reviews...")
    game_reviews['sum_word_cnt'] = game_reviews['review'].apply(lambda x: len(str(x).split()))
    game_reviews['dict_word_cnt'] = game_reviews['review'].apply(lambda x: word_count(str(x)))

    game_reviews.to_csv(game_reviews_filename,index=False)
    print(f"word_count for game reviews collected!")
    print()

@task(task_id='sentiment_analysis')
def sentiment_analysis(ds=None,**kwargs):
    game_article_filename = f"{DATA_PATH}game_articles_{DATE_NOW}.csv"
    game_reviews_filename = f"{DATA_PATH}game_reviews_{DATE_NOW}.csv"

    game_articles = pd.read_csv(game_article_filename)
    game_reviews = pd.read_csv(game_reviews_filename)

    print("getting sentiment for articles...")
    game_articles['polarity_score_summary'] = game_articles['summary'].apply(lambda x: analyze_sentiment(str(x)))
    game_articles["sentiment_summary"] = game_articles["polarity_score_summary"].apply(lambda x: label_polarity(x["compound"]))
        
    game_articles['polarity_score_title'] = game_articles['title'].apply(lambda x: analyze_sentiment(str(x)))
    game_articles["sentiment_title"] = game_articles["polarity_score_title"].apply(lambda x: label_polarity(x["compound"]))
    game_articles.to_csv(game_article_filename, index=False)
    print("sentiment for articles taken!")

    print("getting sentiment for game reviews...")
    game_reviews["sentiment"] = game_reviews["voted_up"].apply(lambda x: "positive" if x else "negative")
    game_reviews.to_csv(game_reviews_filename,index=False)
    print("sentiment for reviews taken!")
# NER
@task(task_id='spacy_ner')
def spacy_ner(ds=None, **kwargs):
    nlp = spacy.load("/model/en_core_web_sm/en_core_web_sm-3.3.0")
    
    def ner(text):
        doc = nlp(text)
        # print("Noun phrases:", [chunk.text for chunk in doc.noun_chunks])
        # print("Verbs:", [token.lemma_ for token in doc if token.pos_ == "VERB"])
        ner = {}
        for entity in doc.ents:
            ner[entity.text] = entity.label_
            print(entity.text, entity.label_)
        return ner

    game_article_filename = f"{DATA_PATH}game_articles_{DATE_NOW}.csv"
    game_reviews_filename = f"{DATA_PATH}game_reviews_{DATE_NOW}.csv"

    game_articles = pd.read_csv(game_article_filename)
    game_reviews = pd.read_csv(game_reviews_filename)
        ################################# TODO: IMPORTANT #########################################
        # you need to find the column where the text/content is located e.g. 'summary' or 'content'
        # and add a conditional logic below
        ###########################################################################################
    print("getting NER for game articles...")
    game_articles['NER'] = game_articles['summary'].apply(lambda x: ner(str(x)))
    game_articles.to_csv(game_article_filename,index=False)
    print("getting NER for game articles complete!")
    print()
        ###########################################################################################
    print("getting NER for game reviews...")
    game_reviews["NER"] = game_reviews["review"].apply(lambda x: ner(str(x)))
    game_reviews.to_csv(game_reviews_filename)
    print("getting NER for game reviews complete!")
    print()


@task(task_id="load_data")
def load_data(ds=None, **kwargs):
    files = os.listdir(DATA_PATH)
    for file in files:
        outfile = f"{DATA_PATH}{file}"
        if not outfile.endswith('.csv'):
            continue
        df = pd.read_csv(outfile)
        csv_buffer = StringIO()

        if "details" in file:
            filename = f"game_details/{file}"
        elif "articles" in file:
            filename = f"game_articles/{file}"
        else:
            filename = f"game_reviews/{file}"

        df.to_csv(csv_buffer)
        upload_string_to_gcs(csv_body=csv_buffer, uploaded_filename=filename)
with DAG(
    'scrapers_proj_test',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        # 'depends_on_past': False,
        # 'email': ['caleb@eskwelabs.com'],
        # 'email_on_failure': False,
        # 'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Pipeline demo',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    t1 = BashOperator(
        task_id="t1_start_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"starting task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )
    t13 = BashOperator(
        task_id="t13_start_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"starting task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )
    t132 = BashOperator(
        task_id="t132_start_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"starting task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )

    t1_end = BashOperator(
        task_id="t1_end_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"ending task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )

    t2_end = BashOperator(
        task_id="t2_end_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"ending task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )
        
    t3_end = BashOperator(
        task_id="t3_end_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"ending task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )

    # t1 >> load_data() >> t2_end

    t1 >> [indigames_plus_feed(),kotaku_feed(), escapist_mag_feed()] >> t13 \
    >> [eurogamer_feed(),rock_paper_sg_feed(),ancient_gaming_feed()] >> t132 \
    >> [scrape_game_details(),scrape_game_reviews()] \
    >> t2_end >> [sentiment_analysis(),spacy_ner(),word_count()] >> load_data() >> t3_end
    # combine_all_articles() >> t1_end >> [scrape_game_details(),scrape_game_reviews()] >> t2_end