from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Create the DAG
dag = DAG(
    'load-prices-paid',
    default_args=default_args,
    description='Load property prices paid data into PostgreSQL database.',
    schedule_interval=None
)

# Define the task: creating the table 'data' in the PostgreSQL database
create_db_task = PostgresOperator(
    task_id='create_db',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE DATABASE prices_paid;
    """,
    dag=dag,
)

# Define the task: creating the table 'data' in the PostgreSQL database
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS prices_paid (
            transaction_id VARCHAR PRIMARY KEY,
            price int,
            transfer_date timestamp,
            postcode text,
            property_type char(1),
            old_new char(1),
            duration char(1),
            address1 text,
            address2 text,
            street text,
            locality text,
            city text,
            district text,
            county text,
            category char(1),
            status char(1)
        )
    """,
    dag=dag,
)

# Define the task: loading data from a CSV file into a PostgreSQL database
load_data_task = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='postgres_default',
    sql="""
        COPY prices_paid FROM '/data/pp-complete.csv' DELIMITER ',' CSV;
    """,
    dag=dag,
)

# Set the order of the tasks
create_db_task >> create_table_task >> load_data_task