from __future__ import print_function
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import models
from airflow.operators import python_operator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from sqlalchemy import create_engine


# Global sql commands
uk_sales_sql = 'select avg("Price") as avg_uk_sales from "price_housing_data"'
price_trend_sql = 'select sum("Price"),Year as total_sales from "price_housing_data" group by Year order by Year desc'
total_sales_sql = 'select count("Transaction_unique_identifier") as total_sales from "price_housing_data" group by Year'
property_type_sql = 'select avg("Price") as avg_property_price from "price_housing_data" group by "Property Type"'
property_area_sql = 'select avg("Price") as avg_property_price from "price_housing_data" group by "District"'

def null_value_check():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    null_count = pd.read_sql_query('select count(*) as null_count_ from "price_housing_data" where "Price" is NULL', con=engine)

    print("nullcount", null_count['null_count_'][0])

    if (null_count['null_count_'][0] == 0):
        print("data has no Nulls")

    return True

def summary_stats():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    df = pd.read_sql_query('select * from "price_housing_data"', con=engine)
    print(df.head())
    stats=df.describe()
    print(stats.head())
    stats.to_sql('price_housing_data_summary', engine, if_exists='replace')

    return True

def calculate_metrics():
    # Establish database connection
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    # Perform queries

    #
    uk_sales = pd.read_sql_query(uk_sales_sql, con=engine)
    #
    price_trend = pd.read_sql_query(price_trend_sql, con=engine)
    #
    total_sales = pd.read_sql_query(total_sales_sql, con=engine)
    #
    property_type = pd.read_sql_query(property_type_sql, con=engine)
    # average price by district
    property_area = pd.read_sql_query(property_area_sql, con=engine)

    # Print "head" of results
    print("Average UK sales")
    print(uk_sales.head())
    print("Price trend")
    print(price_trend.head())
    print("Total sales by year")
    print(total_sales.head())
    print("Average price by property type")
    print(property_type.head())
    print("Average price by district")
    print(property_area.head())

    return True

def save_results():
    return True

default_dag_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with models.DAG(
        '2_metric_calculations',
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        default_args=default_dag_args) as dag:
      
    start = DummyOperator(
        task_id='start',
        dag=dag)
    
    validation_checks_data = python_operator.PythonOperator(
        task_id='null_value_check',
        python_callable=null_value_check)

    analyze_stats = python_operator.PythonOperator(
        task_id='analyze_stats',
        python_callable=summary_stats)
    
    calculate_metrics_calculation = python_operator.PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics)
    
    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start >> validation_checks_data >> analyze_stats >> calculate_metrics_calculation >> end
