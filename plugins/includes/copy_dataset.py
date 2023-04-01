from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

def copy_pp_complete_to_table(dag: DAG) -> PostgresOperator:
    task = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgres_default',
        sql="sql/dataset_into_table.sql",
        dag=dag,
    )   

    return task