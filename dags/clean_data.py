from __future__ import print_function
import datetime
from airflow.utils.dates import days_ago
from airflow import models
from airflow.operators import python_operator
import pendulum
from airflow.operators.dummy import DummyOperator
import logging
import time
import os
import pandas as pd
from sqlalchemy import create_engine


utc_date = days_ago(1)
local_tz = pendulum.timezone("CET")

default_dag_args = {

    "depends_on_past": False,
    "start_date": datetime.datetime(
        utc_date.year, utc_date.month, utc_date.day, tzinfo=local_tz
    )
}

with models.DAG(
        'data_cleaning',
        schedule_interval="0 8 * * *",
        max_active_runs=1,
        catchup=False,
        default_args=default_dag_args) as dag:
      
      
    def process_data():
        df=pd.read_csv('/var/price_paid_records.csv')
        df = df.dropna()
        df=df.drop_duplicates()
        df['year'] = pd.DatetimeIndex(df['Date of Transfer']).year
        df['month'] = pd.DatetimeIndex(df['Date of Transfer']).month
        df['Price'] = df['Price'].astype(float)
        q = df["Price"].quantile(0.75)
        df=df[df["Price"] < q]
        df=df.fillna(df.mean(numeric_only=True).round(1), inplace=False)
        print(df.head())
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
        df.to_sql('price_housing_data', engine,if_exists='replace')

    clean_data = python_operator.PythonOperator(
        task_id='process_data',
        python_callable=process_data)
        
    start = DummyOperator(
        task_id='start',
        dag=dag)
    
    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start>> clean_data>>  end