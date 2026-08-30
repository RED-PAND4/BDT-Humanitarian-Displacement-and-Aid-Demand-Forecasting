from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

DELTA_PACKAGE = 'io.delta:delta-spark_2.12:3.2.0'
SPARK_CONN_ID = 'spark_default'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='3_silver_to_gold',
    default_args=default_args,
    start_date=datetime(2026, 6, 20),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'aggregation', 'spark', 'ml'],
) as dag:

    # ─────────────────────────────────────────────────────────────
    # TIER 1: Primary Gold Feature Aggregations (Reading Silver)
    # ─────────────────────────────────────────────────────────────
    gold_conflict = SparkSubmitOperator(
        task_id='run_gold_conflict_features',
        application='/opt/spark/jobs/gold/silver-to-gold-conflict_features.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    gold_poverty = SparkSubmitOperator(
        task_id='run_gold_poverty_features',
        application='/opt/spark/jobs/gold/silver-to-gold-poverty_features.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    gold_food_security = SparkSubmitOperator(
        task_id='run_gold_food_security_features',
        application='/opt/spark/jobs/gold/silver-to-gold-food_security_features.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    gold_funding = SparkSubmitOperator(
        task_id='run_gold_funding_features',
        application='/opt/spark/jobs/gold/silver-to-gold-funding_features.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    gold_displacement = SparkSubmitOperator(
        task_id='run_gold_displacement',
        application='/opt/spark/jobs/gold/silver-to-gold-displacement.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    gold_aid_demand = SparkSubmitOperator(
        task_id='run_gold_aid_demand_analysis',
        application='/opt/spark/jobs/gold/silver-to-gold-aid.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # ─────────────────────────────────────────────────────────────
    # TIER 2: Intermediate Consolidations & Demand Forecast
    # ─────────────────────────────────────────────────────────────
    gold_country_fact = SparkSubmitOperator(
        task_id='run_gold_country_fact',
        application='/opt/spark/jobs/gold/gold-country_fact.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # gold_host_aggregates = SparkSubmitOperator(
    #     task_id='run_gold_host_aggregates',
    #     application='/opt/spark/jobs/gold/gold-host_aggregates.py',
    #     conn_id=SPARK_CONN_ID,
    #     packages=DELTA_PACKAGE,
    # )

    gold_aid_forecast = SparkSubmitOperator(
        task_id='run_gold_aid_demand_forecast',
        application='/opt/spark/jobs/gold/gold-aid_forecast.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # ─────────────────────────────────────────────────────────────
    # TIER 3: Host Pressure Indices Matrix
    # ─────────────────────────────────────────────────────────────
    gold_host_pressure_indices = SparkSubmitOperator(
        task_id='run_gold_host_pressure_indices',
        application='/opt/spark/jobs/gold/gold-host_pressure_indices.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )

    # ─────────────────────────────────────────────────────────────
    # TIER 4: Machine Learning Forecasting
    # ─────────────────────────────────────────────────────────────
    ml_host_pressure_forecast = SparkSubmitOperator(
        task_id='run_ml_host_pressure_forecast',
        application='/opt/spark/jobs/gold/gold-to-ml-host_pressure_forecast.py',
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
    )


    (
        gold_aid_demand 
        >> gold_aid_forecast
        >> gold_conflict
        >> gold_poverty
        >> gold_food_security
        >> gold_funding
        >> gold_displacement
        >> gold_country_fact
        >> gold_host_pressure_indices
        >> ml_host_pressure_forecast
    )