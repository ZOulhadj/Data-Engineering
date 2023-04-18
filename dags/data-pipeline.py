from airflow import DAG
from datetime import datetime, timedelta

# Subtasks
from includes.create_database_connection import create_airflow_connection
from includes.create_database import create_database
from includes.create_tables import create_table
from includes.download_dataset import download_pp_complete
from includes.copy_dataset import copy_pp_complete_to_table
from includes.dataset_queries import perform_pp_complete_queries
from includes.outputs import output_queries

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


datasets = {
    "pp-2022": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2022.csv",
}

with DAG(
    dag_id='data-pipeline',
    default_args=default_args,
    description='Create airflow connection',
    schedule_interval=None) as dag:

    task_0 = create_airflow_connection(dag)
    task_1 = create_database(dag)
    task_2 = create_table(dag)
    task_3 = download_pp_complete(dag, datasets)
    task_4 = copy_pp_complete_to_table(dag)
    task_5 = perform_pp_complete_queries(dag)
    task_6 = output_queries(dag)

    task_0 >> task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6
