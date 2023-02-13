from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def read_and_load_file():
    # Your code to read and load the file here
    with open('./data/generated_data.csv') as f:
        contents = f.read()
        # Do something with the contents of the file
        print(contents)

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'read_and_load_file_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)

read_and_load_file_task = PythonOperator(
    task_id='read_and_load_file',
    python_callable=read_and_load_file,
    dag=dag,
)
