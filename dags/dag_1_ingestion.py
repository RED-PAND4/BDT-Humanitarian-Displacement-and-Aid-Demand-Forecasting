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

    # 1. Task for UNHCR Population
    kafka_to_bronze_pop = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_population',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_unhcr_population.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 2. Task for UNHCR Solutions
    kafka_to_bronze_sol = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_solutions',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_unhcr_solutions.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 3. Task for World Bank Total Population
    kafka_to_bronze_wb_tot_pop = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_wb_tot_pop',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_tot_pop.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 4. Task for Conflict Events
    kafka_to_bronze_ce = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_ce',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-conflict_events.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 5. Task for Poverty Rate MPI
    kafka_to_bronze_pr = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_pr',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-poverty_rate.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 5. Task for Poverty Rate MPM
    kafka_to_bronze_wb_poverty_MPM = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_wb_poverty_MPM',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_poverty_MPM.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 6. Task for Poverty Rate extreme ($2.15 a day)
    kafka_to_bronze_wb_extreme_poverty = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_wb_extreme_poverty',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_extreme_poverty.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # 7. Task for Food Security
    kafka_to_bronze_fs = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_fs',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-food_security.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )    

    # 8. Task for World Bank GDP per capita
    kafka_to_bronze_wb_gdp = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_wb_gdp',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wb_gdp.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # Task for Currency
    kafka_to_bronze_currency = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_c',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-currency.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task for Food Prices Market Monitor
    kafka_to_bronze_fpmm = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_fpmm',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-food_prices_market_monitor.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    
    # Task for Location
    kafka_to_bronze_location = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_l',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-location.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task for Org Type
    kafka_to_bronze_ot = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_ot',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-org_type.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task for Org
    kafka_to_bronze_org = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_org',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-org.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # Task for Sector
    kafka_to_bronze_sector = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_s',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-sector.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task for WFP Commodity
    kafka_to_bronze_wfpc = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_wfpc',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wfp_commodity.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # Task for WFP Market
    kafka_to_bronze_wfpm = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_stream_wfpm',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-wfp_market.py', # Path inside the spark container
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )
    # # Task for Baseline Population
    # kafka_to_bronze_bp = SparkSubmitOperator(
    #     task_id='run_kafka_to_bronze_stream_bp',
    #     application='/opt/spark/jobs/bronze/kafka-to-bronze-baseline_population.py', # Path inside the spark container
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    # )

    # Task for HDX Humanitarian Needs
    kafka_to_bronze_needs = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_needs',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_hdx_needs.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # Task for HDX IDPs
    kafka_to_bronze_idps = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_idps',
        application='/opt/spark/jobs/bronze/kafka-to-bronze_hdx_idps.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # Task per UNHCR Solutions
    kafka_to_bronze_fun = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_fun',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-funding.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )

    # # Task for national risk
    # kafka_to_bronze_nr = SparkSubmitOperator(
    #     task_id='run_kafka_to_bronze_nr',
    #     application='/opt/spark/jobs/bronze/kafka-to-bronze-national_risk.py',
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    # )

    # Task for Operational Presence
    kafka_to_bronze_op = SparkSubmitOperator(
        task_id='run_kafka_to_bronze_op',
        application='/opt/spark/jobs/bronze/kafka-to-bronze-operational_presence.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0',
    )




    kafka_to_bronze_pop>>kafka_to_bronze_sol>> kafka_to_bronze_wb_tot_pop>>kafka_to_bronze_op>>kafka_to_bronze_fun>>kafka_to_bronze_ce>>kafka_to_bronze_pr>>kafka_to_bronze_wb_poverty_MPM>>kafka_to_bronze_currency>>kafka_to_bronze_fpmm>>kafka_to_bronze_fs>>kafka_to_bronze_location>>kafka_to_bronze_ot>>kafka_to_bronze_org>>kafka_to_bronze_sector>>kafka_to_bronze_wfpc>>kafka_to_bronze_wfpm>>kafka_to_bronze_needs>>kafka_to_bronze_idps