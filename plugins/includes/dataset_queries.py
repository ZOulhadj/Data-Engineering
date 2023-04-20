from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def print_data():
    print("Example queries")

def perform_pp_complete_queries(dag: DAG) -> PythonOperator:
    task = PythonOperator(
        task_id='queries',
        python_callable=print_data,
        dag=dag,
    )
    return task
