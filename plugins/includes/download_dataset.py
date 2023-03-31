from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_data():
    print("Downloading pp-complete dataset")

def download_pp_complete(dag: DAG) -> PythonOperator:
    print_data_task = PythonOperator(
        task_id='download_pp_complete',
        python_callable=print_data,
        dag=dag,
    )

    return print_data_task