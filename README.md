# Game Articles, Reviews and Details Scrape with Airflow
Gaming is one of the biggest industry in todays time. It's recent exponential growth, however, lead to a lot of problems. 

People tend to misinterpret or misunderstand gaming at large and misinformation regarding it is rampant.
Gaming companies also tend to be unregulated simply because no one knows how to deal with it as technology in general, is largely unknown to the public.
People tend to regard gaming as "irrelevant" simply because its just games and not to be taken seriously. Gaming, however, is now a big part of our society and efforts to understand it must be done.

This project aims to paint a clearer picture of the gaming world by gathering game articles, game details and reviews together with sentiment analysis and named entity recognition.
    


## Project Structure
    |—airflow                           <- Main project directory. Contains Airflow files, folders and configurations
        |—dags                          <- Folder where all dags and tasks are configured.
            |—dags.py                   <- File where all tasks and dags are configured.
            |—utils.py                  <- helper functions used by dags.py.
        |—data                          <- Temporary data storage before uploading to the cloud. Emptied after loading.
        |—model                         <- Folder where spacy model used for the project is stored. Do not change if working.
            |—en_core_web_sm
                |—en_core_web_sm-3.3.0  <- spacy model loaded for the project.
        |—docker-compose.yaml           <- required for building docker container, provides specification on building it.
        |—.env                          <- used to configure AIRFLOW_UID
    |—README.MD                         <- The top-level README for developers/collaborators using this project.
---

## How it works

The scraper starts off with scraping rss feeds from six sources:

- ancient gaming
- kotaku
- indigames plus
- escapist magazine
- eurogamer
- rock paper shotgun

The feed returns a `tags` column that highlights the topics of the article. The program loops through all the `tags` until it finds an exact match of the tag in steam's game store directory. Once it does, the `appid` of that game is added as a feature of that article.

If an article's `tags` doesn't match any game in steam, the article's `title` is taken, and using spacy, extracts `noun` from it. All `noun` is looped through until it finds a match in steam's directory again. If an article hasn't found any `appid`, it is dropped.

Using the `appids` scraped, steam api is used to gather the game details and its reviews.

Afterwards, `sentiment_analysis`, `word_count` and, `named entity recognition (NER)` is applied to the texts of both articles and reviews.

## Setup 
Open the command line or terminal

- Clone the repository

```
git clone https://github.com/Hurly119/final_de_project.git
```

- Move to the airflow folder

```
cd airflow
```

- Build the docker image

```
docker compose up
```

- access through http://localhost:8080/

It is necessary for you to configure variables in airflow, and set this manually in the Apache Airflow GUI.

Three keys are required for the airflow variables:
- SERVICE_ACCESS_KEY
- SERVICE_SECRET
- DISCORD_WEBHOOK_API

This can be imported in `admin > variables > + sign (add a new record button)` and then manually provide values for keys above.
Importing a .json file to automatically setup is also possible.

The SERVICE keys are used in `boto3.resource(...)` function to access your cloud storage.

DISCORD_WEBHOOK_API is used for `BashOperator(...)` in dags for notification.   

- if spacy model is not being loaded properly

```
python3 -m spacy download en_core_web_sm
```
- change directory to model

```
cd model
```
- then

```
cp -r spacy_model_dir/en_core_web_sm .
```

