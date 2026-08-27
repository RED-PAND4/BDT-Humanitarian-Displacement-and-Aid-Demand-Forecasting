from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DELTA_PACKAGE = 'io.delta:delta-spark_2.12:3.2.0'
SPARK_CONN_ID = 'spark_default'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='2_bronze_to_silver',
    default_args=default_args,
    start_date=datetime(2026, 6, 20),
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'transformation', 'spark', 'delta'],
) as dag:

    # ─────────────────────────────────────────────────────────────
    # 1. UNHCR Transformations
    # ─────────────────────────────────────────────────────────────
    bronze_to_silver_pop = SparkSubmitOperator(
        task_id='run_bronze_to_silver_population',
        application='/opt/spark/jobs/silver/bronze-to-silver_unhcr_population.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_sol = SparkSubmitOperator(
        task_id='run_bronze_to_silver_solutions',
        application='/opt/spark/jobs/silver/bronze-to-silver_unhcr_solutions.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # ─────────────────────────────────────────────────────────────
    # 2. World Bank Transformations
    # ─────────────────────────────────────────────────────────────
    bronze_to_silver_wb_tot_pop = SparkSubmitOperator(
        task_id='run_bronze_to_silver_wb_tot_pop',
        application='/opt/spark/jobs/silver/bronze-to-silver-wb_tot_pop.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_wb_gdp = SparkSubmitOperator(
        task_id='run_bronze_to_silver_wb_gdp',
        application='/opt/spark/jobs/silver/bronze-to-silver-wb_gdp.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_wb_poverty_mpm = SparkSubmitOperator(
        task_id='run_bronze_to_silver_wb_poverty_mpm',
        application='/opt/spark/jobs/silver/bronze-to-silver-wb_poverty_MPM.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_wb_extreme_poverty = SparkSubmitOperator(
        task_id='run_bronze_to_silver_wb_extreme_poverty',
        application='/opt/spark/jobs/silver/bronze-to-silver-wb_extreme_poverty.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # ─────────────────────────────────────────────────────────────
    # 3. Humdata HDX HAPI Transformations
    # ─────────────────────────────────────────────────────────────
    bronze_to_silver_ce = SparkSubmitOperator(
        task_id='run_bronze_to_silver_conflict_events',
        application='/opt/spark/jobs/silver/bronze-to-silver-conflict_events.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_pr = SparkSubmitOperator(
        task_id='run_bronze_to_silver_poverty_rate',
        application='/opt/spark/jobs/silver/bronze-to-silver-poverty_rate.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_fs = SparkSubmitOperator(
        task_id='run_bronze_to_silver_food_security',
        application='/opt/spark/jobs/silver/bronze-to-silver-food_security.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_fun = SparkSubmitOperator(
        task_id='run_bronze_to_silver_funding',
        application='/opt/spark/jobs/silver/bronze-to-silver-funding.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_idps = SparkSubmitOperator(
        task_id='run_bronze_to_silver_idps',
        application='/opt/spark/jobs/silver/bronze-to-silver_hdx_idps.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_needs = SparkSubmitOperator(
        task_id='run_bronze_to_silver_humanitarian_needs',
        application='/opt/spark/jobs/silver/bronze-to-silver_hdx_needs.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_op = SparkSubmitOperator(
        task_id='run_bronze_to_silver_operational_presence',
        application='/opt/spark/jobs/silver/bronze-to-silver-operational_presence.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_location = SparkSubmitOperator(
        task_id='run_bronze_to_silver_location',
        application='/opt/spark/jobs/silver/bronze-to-silver-location.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_sector = SparkSubmitOperator(
        task_id='run_bronze_to_silver_sector',
        application='/opt/spark/jobs/silver/bronze-to-silver-sector.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    bronze_to_silver_nr = SparkSubmitOperator(
        task_id='run_bronze_to_silver_national_risk',
        application='/opt/spark/jobs/silver/bronze-to-silver-national_risk.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # ─────────────────────────────────────────────────────────────
    # Execution Sequence (Sequential Execution for RAM)
    # ─────────────────────────────────────────────────────────────
    (
        bronze_to_silver_pop
        >> bronze_to_silver_sol
        >> bronze_to_silver_wb_tot_pop
        >> bronze_to_silver_wb_gdp
        >> bronze_to_silver_wb_poverty_mpm
        >> bronze_to_silver_wb_extreme_poverty
        >> bronze_to_silver_ce
        >> bronze_to_silver_pr
        >> bronze_to_silver_fs
        >> bronze_to_silver_fun
        >> bronze_to_silver_idps
        >> bronze_to_silver_needs
        >> bronze_to_silver_op
        >> bronze_to_silver_location
        >> bronze_to_silver_sector
        >> bronze_to_silver_nr
    )