from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_data():
    print("This is the output DAG")

def output_queries(dag: DAG) -> PythonOperator:
    task = PythonOperator(
        task_id='output',
        python_callable=print_data,
        dag=dag,
    )

    return task