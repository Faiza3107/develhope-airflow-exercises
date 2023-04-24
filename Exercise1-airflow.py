from airflow import DAG #we imported the definition of DAG from airflow 
from airflow.operators.bash_operator import BashOperator #we imported the operator we need 
from datetime import datetime, timedelta
#an operator is airflows way of talking to some sort of language
#python operator
#postgres operator
#bash operator #-tells the operator to run this bash command
#the default arg is basic information we keep in our airflow, start date, when to start it, 
#email on failure, means on whether it should send an email when its wrong 

#we defined default arguments 
default_dag_args={
    'start_date':datetime(2022,1,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':1
}

#lets define our dag

with DAG("First_DAG", schedule_interval = None, default_args = default_dag_args) as dag:

    #here at this level, we do define our tasks of the DAG , we created a simple task 
    task_0 = BashOperator(task_id = 'bash_task', bash_command = "echo 'command executed from Bash Operator' ")
    task_1 = BashOperator(task_id = 'bash_task_move_data', bash_command = "cp\Users\PC\data_engineering\Airflow\DATA_CENTER\DATA_LAKE/dataset_raw.txt  \Users\PC\data_engineering\Airflow\DATA_CENTER\CLEAN_DATA")
    task_2 = BashOperator(task_id = 'bash_task_move_data', bash_command = "")
    
    #in the end we have to write the dependencies of the tasks
    #task_0>>task_1>>task_2