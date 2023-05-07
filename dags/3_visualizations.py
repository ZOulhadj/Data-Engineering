# Import necessary libraries and modules
import pandas as pd
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum
from airflow import models
from airflow.operators import python_operator
from airflow.operators.dummy import DummyOperator
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from airflow import models
# date
utc_date = days_ago(1)
local_tz = pendulum.timezone("CET")

# Set default arguments for the DAG
default_dag_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG using a context manager
with models.DAG(
    '3_visualizations',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    default_args=default_dag_args) as dag:
    
    # Function to query data from PostgreSQL and save it to a CSV file
    def queryPostgres():
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
        df = pd.read_sql_query('select * from "price_housing_data"',con=engine)
        print(df.head())
        df.to_csv('postgresqldata.csv')
        print("-------Data Saved------")

    # Function to read data from the CSV file and insert it into Elasticsearch
    def insertElasticseearch():
        es = Elasticsearch("my_elasticsearch")
        df=pd.read_csv('postgresqldata.csv')
        for i,r in df.iterrows():
            doc=r.to_json()
            res=es.index(index="frompostgresql", doc_type="doc",body=doc)
            print(res)

    # Function to read data from the CSV file and insert it into Elasticsearch
    getData = python_operator.PythonOperator(
        task_id='QueryPostgreSQL', 
        python_callable=queryPostgres)

    insertData = python_operator.PythonOperator(
        task_id='InsertDataElasticsearch', 
        python_callable=insertElasticseearch)
    
    # Define tasks for the DAG using DummyOperato
    start = DummyOperator(
        task_id='start',
        dag=dag)
    # End Dag
    end = DummyOperator(
        task_id='end',
        dag=dag
    )
    
    # Set the order of task execution using the bitshift operator
    start >> getData >> insertData >> end
