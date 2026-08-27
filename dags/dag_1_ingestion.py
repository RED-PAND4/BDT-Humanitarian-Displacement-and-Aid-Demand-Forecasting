from datetime import datetime
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_PACKAGES = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0'
SPARK_CONN_ID = 'spark_default'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='1_kafka_to_bronze',
    default_args=default_args,
    start_date=datetime(2026, 6, 20),
    catchup=False,
    tags=['bronze', 'ingestion', 'spark', 'kafka'],
) as dag:

    # ─────────────────────────────────────────────────────────────
    # 1. UNHCR APIs
    # ─────────────────────────────────────────────────────────────
    kafka_to_bronze_pop = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_population',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_unhcr_population.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_sol = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_solutions',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_unhcr_solutions.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    # ─────────────────────────────────────────────────────────────
    # 2. World Bank APIs
    # ─────────────────────────────────────────────────────────────
    kafka_to_bronze_wb_tot_pop = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_wb_tot_pop',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_tot_pop.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_wb_gdp = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_wb_gdp',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_gdp.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_wb_poverty_mpm = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_wb_poverty_mpm',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_poverty_MPM.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_wb_extreme_poverty = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_wb_extreme_poverty',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_extreme_poverty.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    # ─────────────────────────────────────────────────────────────
    # 3. Humdata HDX HAPI APIs
    # ─────────────────────────────────────────────────────────────
    kafka_to_bronze_ce = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_conflict_events',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-conflict_events.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_pr = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_poverty_rate',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-poverty_rate.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_fs = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_food_security',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-food_security.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_fun = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_funding',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-funding.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_idps = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_idps',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_hdx_idps.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_needs = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_humanitarian_needs',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_hdx_needs.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_op = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_operational_presence',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-operational_presence.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_location = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_location',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-location.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_sector = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_sector',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-sector.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    kafka_to_bronze_nr = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_national_risk',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-national_risk.py',
        conn_id=SPARK_CONN_ID,
        packages=DEFAULT_PACKAGES,
    )

    # ─────────────────────────────────────────────────────────────
    # Execution Sequence (Sequential Execution for RAM)
    # ─────────────────────────────────────────────────────────────
    (
        kafka_to_bronze_pop
        >> kafka_to_bronze_sol
        >> kafka_to_bronze_wb_tot_pop
        >> kafka_to_bronze_wb_gdp
        >> kafka_to_bronze_wb_poverty_mpm
        >> kafka_to_bronze_wb_extreme_poverty
        >> kafka_to_bronze_ce
        >> kafka_to_bronze_pr
        >> kafka_to_bronze_fs
        >> kafka_to_bronze_fun
        >> kafka_to_bronze_idps
        >> kafka_to_bronze_needs
        >> kafka_to_bronze_op
        >> kafka_to_bronze_location
        >> kafka_to_bronze_sector
        >> kafka_to_bronze_nr
    )