# TODO: Here we should find a way to combine all unique DAGs into one

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.dummy import DummyOperator

default_dag_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with models.DAG(
        'complete_pipeline',
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        default_args=default_dag_args) as dag:
      
    start = DummyOperator(task_id='start', dag=dag)

    trigger_1 = TriggerDagRunOperator(
        task_id="initialization_trigger",
        trigger_dag_id="1_initialization",  # Ensure this equals the dag_id of the DAG to trigger
    )

    trigger_2 = TriggerDagRunOperator(
        task_id="metric_calculations_trigger",
        trigger_dag_id="2_metric_calculations",  # Ensure this equals the dag_id of the DAG to trigger
    )

    trigger_3 = TriggerDagRunOperator(
        task_id="visualizations_trigger",
        trigger_dag_id="3_visualizations",  # Ensure this equals the dag_id of the DAG to trigger
    )

    end = DummyOperator(task_id='end', dag=dag)

    start >> trigger_1 >> trigger_2 >> trigger_3 >> end
