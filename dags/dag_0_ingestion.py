import os
import sys
from airflow import DAG
from airflow.models.param import Param
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

# with DAG(
#     dag_id='0_getApi',
#     default_args=default_args,
#     #schedule=timedelta(minutes=2),
#     start_date=datetime(2026, 6, 20),
#     catchup=False,
#     max_active_runs=1,
# ) as dag:

with DAG(
    dag_id="0_getApi",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    #schedule=None,  # Manual triggers only
    # CRITICAL: This ensures {{ params.yearFrom }} stays an int instead of a string
    render_template_as_native_obj=True, 
    params={
        "yearFrom": Param(
            default=2000, 
            type="integer", 
            minimum=1800, 
            maximum=2100, 
            title="Start Year"
        ),
        "yearTo": Param(
            default=2025, 
            type="integer", 
            minimum=1800, 
            maximum=2100, 
            title="End Year"
        ),
    },
) as dag:

    getapi = BashOperator(
        task_id='getapi_task',
        # We pass the parameters as arguments at the end of the python call
        bash_command=(
            'pip install confluent-kafka && '
            'python3 /opt/spark/jobs/getapi/main.py '
            '--year_from {{ params.yearFrom }} '
            '--year_to {{ params.yearTo }}'
        ),
    )

    getapi

# # Task 0: Executed via standard python3 command
#     getapi = BashOperator( 
#         task_id='getapi_task', 
#         bash_command='pip install confluent-kafka && python3 /opt/spark/jobs/getapi/main.py',
#     ) 

#     getapi
