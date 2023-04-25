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
        'summary_stats',
        schedule_interval="0 8 * * *",
        max_active_runs=1,
        catchup=False,
        default_args=default_dag_args) as dag:
      
      
    def summary_stats():
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
        df = pd.read_sql_query('select * from "price_housing_data"',con=engine)
        print(df.head())
        stats=df.describe()
        stats.head()
        stats.to_sql('price_housing_data_summary', engine,if_exists='replace')
    
    def calculate_metrics():
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
        uk_sales = pd.read_sql_query('select avg("Price") as avg_uk_sales from "price_housing_data"',con=engine)
        print(uk_sales.head())
        price_trend = pd.read_sql_query('select sum("Price"),Year as total_sales from "price_housing_data" group by Year order by Year desc',con=engine)
        print(price_trend.head())
        total_sales = pd.read_sql_query('select count("Transaction_unique_identifier") as total_sales from "price_housing_data" group by Year',con=engine)
        print(total_sales.head())
        property_dist = pd.read_sql_query('select avg("Price") as avg_property_price from "price_housing_data" group by "Property Type"',con=engine)
        print(property_dist.head())
    
    def validation_checks():
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
        null_count = pd.read_sql_query('select count(*) as null_count_ from "price_housing_data" where "Price" is NULL',con=engine)
        print("nullcount",null_count['null_count_'][0])
        if(null_count['null_count_'][0]==0):
            print("data has no Nulls")

    analyze_stats = python_operator.PythonOperator(
        task_id='analyze_stats',
        python_callable=summary_stats)
    
    calculate_metrics_calculation = python_operator.PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics)
    
    validation_checks_data = python_operator.PythonOperator(
        task_id='validation_checks',
        python_callable=validation_checks)
        
    start = DummyOperator(
        task_id='start',
        dag=dag)
    
    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start>> validation_checks_data>> analyze_stats>> calculate_metrics_calculation >>  end
