from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

def create_database(dag: DAG) -> PostgresOperator:
    task = PostgresOperator(
        task_id='create_database',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE DATABASE prices_paid;
        """,
        autocommit=True,
        dag=dag,
    )

    return task
