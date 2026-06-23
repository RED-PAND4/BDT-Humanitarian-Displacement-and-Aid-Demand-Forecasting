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

    # # Task C: Clean the data (Bronze -> Silver)
    # bronze_to_silver = SparkSubmitOperator(
    #     task_id='bronze_to_silver_task',
    #     application='/opt/spark/jobs/silver/bronze-to-silver.py',
    #     conn_id='spark_default',
    #     packages='io.delta:delta-spark_2.12:3.2.0',
    # )
    # Task C: Clean the data (Bronze -> Silver)
    bronze_to_silver_currency = SparkSubmitOperator(
        task_id='bronze_to_silver_task_c',
        application='/opt/spark/jobs/silver/bronze-to-silver-currency.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_fpmm = SparkSubmitOperator(
        task_id='bronze_to_silver_task_fpmm',
        application='/opt/spark/jobs/silver/bronze-to-silver-food_prices_market_monitor.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_fs = SparkSubmitOperator(
        task_id='bronze_to_silver_task_fs',
        application='/opt/spark/jobs/silver/bronze-to-silver-food_security.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_location = SparkSubmitOperator(
        task_id='bronze_to_silver_task_l',
        application='/opt/spark/jobs/silver/bronze-to-silver-location.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_ot = SparkSubmitOperator(
        task_id='bronze_to_silver_task_ot',
        application='/opt/spark/jobs/silver/bronze-to-silver-org_type.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_org = SparkSubmitOperator(
        task_id='bronze_to_silver_task_org',
        application='/opt/spark/jobs/silver/bronze-to-silver-org.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_pr = SparkSubmitOperator(
        task_id='bronze_to_silver_task_pr',
        application='/opt/spark/jobs/silver/bronze-to-silver-poverty_rate.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_sector = SparkSubmitOperator(
        task_id='bronze_to_silver_task_s',
        application='/opt/spark/jobs/silver/bronze-to-silver-sector.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_bp = SparkSubmitOperator(
        task_id='bronze_to_silver_task_bp',
        application='/opt/spark/jobs/silver/bronze-to-silver-baseline_population.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_wfpc = SparkSubmitOperator(
        task_id='bronze_to_silver_task_wfpc',
        application='/opt/spark/jobs/silver/bronze-to-silver-wfp_commodity.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    bronze_to_silver_wfpm = SparkSubmitOperator(
        task_id='bronze_to_silver_task_wfpm',
        application='/opt/spark/jobs/silver/bronze-to-silver-wfp_market.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )
    silver_needs = SparkSubmitOperator(
        task_id='silver_hdx_needs',
        application='/opt/spark/jobs/silver/bronze-to-silver_hdx_needs.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    silver_idps = SparkSubmitOperator(
        task_id='silver_hdx_idps',
        application='/opt/spark/jobs/silver/bronze-to-silver_hdx_idps.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    silver_pop = SparkSubmitOperator(
        task_id='silver_unhcr_pop',
        application='/opt/spark/jobs/silver/bronze-to-silver_unhcr_population.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    silver_sol = SparkSubmitOperator(
        task_id='silver_unhcr_sol',
        application='/opt/spark/jobs/silver/bronze-to-silver_unhcr_solutions.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0',
    )

    
    [bronze_to_silver_bp, bronze_to_silver_currency, bronze_to_silver_fpmm, bronze_to_silver_fs, bronze_to_silver_location, bronze_to_silver_ot, bronze_to_silver_org, bronze_to_silver_pr, bronze_to_silver_sector, bronze_to_silver_wfpc, bronze_to_silver_wfpm,silver_needs, silver_idps, silver_pop, silver_sol ]
    #bronze_to_silver_currency>>bronze_to_silver_fpmm>>bronze_to_silver_fs>>bronze_to_silver_location>>bronze_to_silver_ot>>bronze_to_silver_org>>bronze_to_silver_pr>>bronze_to_silver_sector>>bronze_to_silver_wfpc>>bronze_to_silver_wfpm