import requests
import time
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

#to make your functions accept parameters, you must defind key word arguments
#by writing kwargs your airflow task is exoecting dictionary or arguments 
def get_data(**kwargs):
    tickers = kwargs['ticker']
    api_key = "YB28YSE6YOOIX286"
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=' + tickers + '&apikey='+ api_key
    r = requests.get(url)


default_dag_args={
    'start_date':datetime(2022,9,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':1
}

with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup = False,  default_args = default_dag_args) as dag_python:

#here I define my tasks
  task_0 = PythonOperator(task_id = "get_market_data", python_callable  = get_data, op_kwargs = {'tickers' : []})