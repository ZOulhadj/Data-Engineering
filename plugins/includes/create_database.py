from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# SELECT 'CREATE DATABASE prices_paid' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prices_paid')

def create_database(dag: DAG) -> PostgresOperator:
    task = PostgresOperator(
        task_id='create_database',
        postgres_conn_id='postgres_default',
        sql="sql/create_database.sql",
        autocommit=True,
        dag=dag,
    )

    return task
