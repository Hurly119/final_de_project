# NER/OSM Data Pipeline with Apache Airflow & DAGs 
The data pipeline is catered to providing accesible, transformed, and normalized data for NLP/NER and Geospatial modelling data.

## Contribution Guidelines



## Project Structure
|—airflow                   <- Main project directory. Contains Airflow files and configurations
    |—dags                  <- Folder where all dags and tasks are configured
    |—data                  <- Temporary data storage before uploading to the cloud. 
    |—model                 <- Folder where spacy model used for the project is stored.
    |—docker-compose.yaml   <- required for building docker container, provides specification on how it should be built.
    |—.env                  <- used to configure AIRFLOW_UID
|—README.MD                 <- The top-level README for developers/collaborators using this project.


## Main use 
The relevant files are stored in the `dags` folder, with the `test_dag.py` containing a sample dag for testing. You can modify the name, and description of these dags by modifying the relevant strings in between the bash operators.

The main scrapers to be utilized are `scraper_dag.py` and `scraper_v2_dag.py`, which contain the main tasks and the runnable files. Street image.py is a sample script that obtains and stores the images of OSM, and is constructed for your reference. 

You can also increase the number of images / articles you scrape by modifying the `LIMIT` category in their respective queries.

The pipeline has five sets of tasks. It scrapes data from selected newspapers, transforms them by adding NER and word_count columns as a way to prepare the headlines/summaries for analysis, then uploads them to a GCS bucket defined by the credentials provided. Following this, the local copies of the data on your machine are wiped, minimizing redundancy and data costs every time the scraper DAG is run. 

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

This is found in `admin > variables > + sign (add a new record button)` and then manually provide values for keys above.
Importing a .json file to automatically setup is also possible.

The SERVICE keys are used in `boto3.resource(...)` function to access your cloud storage.

DISCORD_WEBHOOK_API is used for `BashOperator(...)` in dags for notification.   




