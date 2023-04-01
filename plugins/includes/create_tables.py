from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# todo: need to switch to prices_paid database before creating table

def create_table(dag: DAG) -> PostgresOperator:
    task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="sql/create_table.sql",
        dag=dag,
    )

    return task
