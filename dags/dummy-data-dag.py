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

# Create the DAG
dag = DAG(
    'postgres_load_dag',
    default_args=default_args,
    description='A data pipeline example to load data into a PostgreSQL database using the PostgresOperator in Apache Airflow',
    schedule_interval=timedelta(hours=1),
)

# Define the task: creating the table 'data' in the PostgreSQL database
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS data (
            Name VARCHAR,
            Age INT,
            Country VARCHAR
        )
    """,
    dag=dag,
)

# Define the task: loading data from a CSV file into a PostgreSQL database
load_data_task = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='postgres_default',
    sql="""
        COPY data FROM '/data/generated_data.csv' DELIMITER ',' CSV HEADER;
    """,
    dag=dag,
)

# Define the task: printing the data loaded by load_data_task
def print_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM data")
    records = cursor.fetchall()
    for record in records:
        print(record)

print_data_task = PythonOperator(
    task_id='print_data',
    python_callable=print_data,
    dag=dag,
)


# Set the order of the tasks
create_table_task >> load_data_task >> print_data_task 