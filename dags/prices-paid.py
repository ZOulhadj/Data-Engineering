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
    'prices-paid',
    default_args=default_args,
    description='Query prices paid dataset through PostgreSQL',
    schedule_interval=None
)

# Define the task: printing the data loaded by load_data_task
def print_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT count(*) AS exact_count FROM prices_paid")
    records = cursor.fetchall()
    print(records)

print_data_task = PythonOperator(
    task_id='print_data',
    python_callable=print_data,
    dag=dag,
)


# Set the order of the tasks
print_data_task