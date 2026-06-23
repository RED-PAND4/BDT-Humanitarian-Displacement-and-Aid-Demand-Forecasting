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

    # # Task A: Run the Spark Streaming script
    # kafka_to_bronze = SparkSubmitOperator(
    #     task_id='run_kafka_to_bronze_stream',
    #     application='/opt/spark/jobs/bronze/kafka_to_bronze_humanitarian_needs.py', # Path inside the spark container
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    # )

    # kafka_to_bronze

    # 1. Task per HDX Humanitarian Needs
    kafka_to_bronze_needs = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_needs',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_hdx_needs.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 2. Task per HDX IDPs
    kafka_to_bronze_idps = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_idps',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_hdx_idps.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 3. Task per UNHCR Population
    kafka_to_bronze_pop = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_population',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_unhcr_population.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 4. Task per UNHCR Solutions
    kafka_to_bronze_sol = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_solutions',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_unhcr_solutions.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    #kafka_to_bronze_needs >> kafka_to_bronze_idps >> kafka_to_bronze_pop >> kafka_to_bronze_sol
    [kafka_to_bronze_needs, kafka_to_bronze_idps, kafka_to_bronze_pop, kafka_to_bronze_sol]
