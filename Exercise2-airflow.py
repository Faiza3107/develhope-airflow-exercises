from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

#we write here our python function. This function should print the current datetime

def pyhton_first_function():
    print("Hello I am a python function called by airflow")


default_dag_args={
    'start_date':datetime(2022,9,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':1
}
#crontab notation can be useful https://crontab.guru/#0_0_*_*_1
#creating a DAG that calls the pyhton logic that I had created above 
with DAG("first_python_dag", schedule_interval = '@daily', catchup = False, default_args = default_dag_args) as dag_python:
    #we are defining our tasks, we always need a task id, python callable is what i want to call, and this can be name of the function you created 
    task_0 = PythonOperator(task_id = "first_pyhton_task", python_callable = pyhton_first_function)
