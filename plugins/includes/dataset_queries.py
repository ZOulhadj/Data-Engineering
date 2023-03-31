from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

def perform_pp_complete_queries(dag: DAG) -> PostgresOperator:
    task = PostgresOperator(
        task_id='queries',
        postgres_conn_id='postgres_default',
        sql="""
        """,
        dag=dag,
    )   

    return task