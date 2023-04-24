#interaction with postgres database in airflow 
import time
import json
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator 
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = { 'owner': 'airflow',
'retries': 1, 'retry_delay': timedelta(minutes=5), }

#our sql logic defined
create_query = """
DROP TABLE IF EXISTS tableFaiza;
CREATE TABLE tableFaiza(id INT NOT NULL, name VARCHAR(250), age INT NOT NULL);
"""
insert_data_query = """
INSERT INTO tableFaiza(id, name, age)
values(1, 'Mary', 28), (2,'Jane', 28), (3, 'John', 35)
"""

calculating_averag_age = """
SELECT AVG(age)
FROM tableFaiza
"""


dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1))
create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "test_airflow")

insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "test_airflow")

group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_averag_age, dag = dag_postgres, postgres_conn_id = "test_airflow")

create_table >> insert_data >> group_data
#create_table = PostgresOperator(task_id = "creation_of_table", sql = "create_qurey")