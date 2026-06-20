from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='1_always_on_ingestion',
    default_args=default_args,
    #schedule='@once',
    start_date=datetime(2026, 6, 20),
    catchup=False,
) as dag:

    # Task A: Run the Spark Streaming script
    kafka_to_bronze = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream',
        application='/opt/spark/jobs/bronze/kafka-to-bronze2.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    kafka_to_bronze