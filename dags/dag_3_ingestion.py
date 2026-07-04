from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='3_silver_to_gold',
    default_args=default_args,
    #schedule=timedelta(minutes=5),
    start_date=datetime(2026, 6, 20),
    catchup=False,
    max_active_runs=1,
) as dag:


    # Task D: Aggregate the data (Silver -> Gold)
    silver_to_gold_t = SparkSubmitOperator(
        task_id='silver_to_gold_t',
        application='/opt/spark/jobs/gold/silver-to-gold-test2.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    silver_to_gold_ad = SparkSubmitOperator(
        task_id='silver_to_gold_ad',
        application='/opt/spark/jobs/gold/silver-to-gold_aid_demand.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    silver_to_gold_t>>silver_to_gold_ad