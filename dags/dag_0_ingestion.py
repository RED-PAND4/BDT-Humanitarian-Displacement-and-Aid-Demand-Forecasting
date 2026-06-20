import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='0_lakehouse_processing',
    default_args=default_args,
    #schedule=timedelta(minutes=2),
    start_date=datetime(2026, 6, 20),
    catchup=False,
    max_active_runs=1,
) as dag:

# Task 0: Executed via standard python3 command
    getapi = BashOperator( 
        task_id='getapi_task', 
        bash_command='pip install confluent-kafka && python3 /opt/spark/jobs/getapi/main.py',
    ) 

    getapi
