# TODO: Here we should find a way to combine all unique DAGs into one

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.dummy import DummyOperator

# Set default arguments for the DAG
default_dag_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG using a context manager
with models.DAG(
        'complete_pipeline',
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        default_args=default_dag_args) as dag:
    
    # Define start and end tasks using DummyOperator   
    start = DummyOperator(task_id='start', dag=dag)
    
    # Define a TriggerDagRunOperator task to trigger the "1_initialization" DAG
    trigger_1 = TriggerDagRunOperator(
        task_id="initialization_trigger",
        trigger_dag_id="1_initialization",  # Ensure this equals the dag_id of the DAG to trigger
    )
    
    # Define a TriggerDagRunOperator task to trigger the "2_metric_calculations" DAG
    trigger_2 = TriggerDagRunOperator(
        task_id="metric_calculations_trigger",
        trigger_dag_id="2_metric_calculations",  # Ensure this equals the dag_id of the DAG to trigger
    )

    # Define a TriggerDagRunOperator task to trigger the "3_visualizations" DAG
    trigger_3 = TriggerDagRunOperator(
        task_id="visualizations_trigger",
        trigger_dag_id="3_visualizations",  # Ensure this equals the dag_id of the DAG to trigger
    )
    # Define the end task using DummyOperator
    end = DummyOperator(task_id='end', dag=dag)

    # Set the order of task execution using the bitshift operator
    start >> trigger_1 >> trigger_2 >> trigger_3 >> end
