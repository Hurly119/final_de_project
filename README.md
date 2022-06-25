# NER/OSM Data Pipeline with Apache Airflow & DAGs 
The data pipeline is catered to providing accesible, transformed, and normalized data for NLP/NER and Geospatial modelling data.

## Contribution Guidelines

- Have a Look at the [project structure](#project-structure) and [folder overview](#folder-overview) 
- If you're creating a task, Go to the task folder and create a new folder with the below naming convention and add a README.md with task details and goals to help other contributors understand
  - Task Folder Naming Convention : _task-n-taskname.(n is the task number)_ ex: task-1-data-analysis, task-2-model-deployment etc.
  - Create a README.md with a table containing information table about all contributions for the task.
- If you're contributing for a task, please make sure to store in relavant location and update the README.md information table with your contribution details.
- Make sure your File names(jupyter notebooks, python files, data sheet file names etc) has proper naming to help others in easily identifing them.
- Please restrict yourself from creating unnessesary folders other than in 'tasks' folder (as above mentioned naming convention) to avoid confusion.


## Project Structure
    |—airflow                           <- Main project directory. Contains Airflow files and configurations
        |—dags                          <- Folder where all dags and tasks are configured.
            |—dags.py                   <- File where all tasks and dags are configured.
            |—utils.py                  <- helper functions used by dags.py.
        |—data                          <- Temporary data storage before uploading to the cloud. 
        |—model                         <- Folder where spacy model used for the project is stored.
            |—en_core_web_sm
                |—en_core_web_sm-3.3.0  <- spacy model loaded for the project.
        |—docker-compose.yaml           <- required for building docker container, provides specification on building it.
        |—.env                          <- used to configure AIRFLOW_UID
    |—README.MD                         <- The top-level README for developers/collaborators using this project.
---
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

This can be imported in `admin > variables > + sign (add a new record button)` and then manually provide values for keys above.
Importing a .json file to automatically setup is also possible.

The SERVICE keys are used in `boto3.resource(...)` function to access your cloud storage.

DISCORD_WEBHOOK_API is used for `BashOperator(...)` in dags for notification.   




