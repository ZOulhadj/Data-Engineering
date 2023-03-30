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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'output',
    default_args=default_args,
    description='The output DAG',
    schedule_interval=timedelta(hours=1),
)

def print_data():
    print("This is the output DAG")

print_data_task = PythonOperator(
    task_id='output',
    python_callable=print_data,
    dag=dag,
)