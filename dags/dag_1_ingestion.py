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
    dag_id='1_kafka_to_bronze',
    default_args=default_args,
    #schedule='@once',
    start_date=datetime(2026, 6, 20),
    catchup=False,
) as dag:

    # # Task A: Run the Spark Streaming script
    # kafka_to_bronze = SparkSubmitOperator(
    #     task_id='run_kafka_to_bronze_stream',
    #     application='/opt/spark/jobs/bronze/kafka-to-bronze2.py', # Path inside the spark container
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    # )
    # Task A1: Run the Spark Streaming script
    kafka_to_bronze_currency = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_c',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-currency.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A2: Run the Spark Streaming script
    kafka_to_bronze_fpmm = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_fpmm',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-food_prices_market_monitor.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A3: Run the Spark Streaming script
    kafka_to_bronze_fs = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_fs',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-food_security.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A4: Run the Spark Streaming script
    kafka_to_bronze_location = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_l',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-location.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A5: Run the Spark Streaming script
    kafka_to_bronze_ot = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_ot',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-org_type.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A6: Run the Spark Streaming script
    kafka_to_bronze_org = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_org',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-org.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A7: Run the Spark Streaming script
    kafka_to_bronze_pr = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_pr',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-poverty_rate.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A8: Run the Spark Streaming script
    kafka_to_bronze_sector = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_s',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-sector.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A9: Run the Spark Streaming script
    kafka_to_bronze_wfpc = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_wfpc',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wfp_commodity.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A10: Run the Spark Streaming script
    kafka_to_bronze_wfpm = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_wfpm',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wfp_market.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task A10: Run the Spark Streaming script
    kafka_to_bronze_bp = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_bp',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-baseline_population.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
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
    # 4. Task
    kafka_to_bronze_ce = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_ce',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-conflict_events.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # 4. Task per UNHCR Solutions
    kafka_to_bronze_fun = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_fun',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-funding.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # 4. Task per UNHCR Solutions
    kafka_to_bronze_nr = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_nr',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-national_risk.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # 4. Task per UNHCR Solutions
    kafka_to_bronze_op = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_op',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-operational_presence.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    kafka_to_bronze_nr>>kafka_to_bronze_fun>>kafka_to_bronze_ce>>kafka_to_bronze_bp>>kafka_to_bronze_currency>>kafka_to_bronze_fpmm>>kafka_to_bronze_fs>>kafka_to_bronze_location>>kafka_to_bronze_ot>>kafka_to_bronze_org>>kafka_to_bronze_pr>>kafka_to_bronze_sector>>kafka_to_bronze_wfpc>>kafka_to_bronze_wfpm>>kafka_to_bronze_needs>>kafka_to_bronze_idps>>kafka_to_bronze_pop>>kafka_to_bronze_sol