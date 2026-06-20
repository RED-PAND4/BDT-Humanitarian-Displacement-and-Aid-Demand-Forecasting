from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='2_lakehouse_processing',
    default_args=default_args,
    #schedule=timedelta(minutes=1), 
    start_date=datetime(2026, 6, 20),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task C: Clean the data (Bronze -> Silver)
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver_task',
        application='/opt/spark/jobs/silver/bronze-to-silver.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    bronze_to_silver