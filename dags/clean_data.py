from __future__ import print_function
import datetime
from airflow.utils.dates import days_ago
from airflow import models
from airflow.operators import python_operator
import pendulum
from airflow.operators.dummy import DummyOperator
import opendatasets as od
import json,subprocess
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
      
    def download_house_price_data():
        api_dict = {"username":"taylorh122","key":"dac660c475435516f8cedd8924069d7c"} # set this in .env
        with open(f"kaggle.json", "w", encoding='utf-8') as f:
            json.dump(api_dict, f)
        cmd = f"chmod 600 kaggle.json"
        output = subprocess.check_output(cmd.split(" "))
        output = output.decode(encoding='UTF-8')
        print("downloading...")
        print(output)
        od.download("https://www.kaggle.com/datasets/hm-land-registry/uk-housing-prices-paid")
        return 1    

    def process_data():
        df=pd.read_csv('./uk-housing-prices-paid/price_paid_records.csv', nrows=100000) # limiting row numbers to 100000 because of RAM/Memory needs
        df = df.dropna()
        df=df.drop_duplicates()
        col = df.columns.str.split(',')
        df=df.reindex(columns=df.columns.repeat(col.str.len()))
        df.columns=sum(col.tolist(),[])
        df.rename(columns={ df.columns[0]: "Transaction_unique_identifier" }, inplace = True)
        df.rename(columns={ df.columns[1]: "Price" }, inplace = True)
        df.rename(columns={ df.columns[2]: "Date of Transfer" }, inplace = True)
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
    
    download_data = python_operator.PythonOperator(
        task_id='download_data',
        python_callable=download_house_price_data)   
         
    start = DummyOperator(
        task_id='start',
        dag=dag)
    
    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start>> download_data >> clean_data>>  end