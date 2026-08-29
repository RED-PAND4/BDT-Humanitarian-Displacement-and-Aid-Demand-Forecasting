#import os
#import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# ─────────────────────────────────────────────────────────────
# 1. Default Arguments
# ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

# ─────────────────────────────────────────────────────────────
# 2. DAG Definition & UI Parameters
# ─────────────────────────────────────────────────────────────
# Ingestion DAG: Download raw data from external APIs and send it to Kafka
with DAG(
    dag_id="0_getApi",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    #schedule=None,  # Manual triggers only
    # CRITICAL: This ensures {{ params.yearFrom }} stays an int instead of a string
    render_template_as_native_obj=True, 
    tags=['ingestion', 'api', 'kafka', 'bronze'],
    # Parameters configurable directly from the Airflow UI interface to the trigger
    params={
        "yearFrom": Param(
            default=2020, 
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

    # ─────────────────────────────────────────────────────────────
    # 3. Task Execution
    # ─────────────────────────────────────────────────────────────
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
