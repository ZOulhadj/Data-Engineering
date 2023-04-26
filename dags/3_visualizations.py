from __future__ import print_function
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import models
from airflow.operators import python_operator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from sqlalchemy import create_engine


default_dag_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with models.DAG(
        '3_visualizations',
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        default_args=default_dag_args) as dag:
      
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)

    start >>  end
