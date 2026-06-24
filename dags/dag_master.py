# /opt/airflow/dags/master_orchestrator.py
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='master_pipeline_controller',
    default_args=default_args,
    schedule=None,  # Or set a schedule here to run the whole chain automatically
    catchup=False,
) as dag:

    # 1. Define the trigger tasks
    trigger_dag_0 = TriggerDagRunOperator(
        task_id='trigger_dag_0',
        trigger_dag_id='0_getApi',          # Must match the dag_id inside your 1st file
        wait_for_completion=True,          # Tells the controller to wait until it finishes
        poke_interval=60,
    )

    trigger_dag_1 = TriggerDagRunOperator(
        task_id='trigger_dag_1',
        trigger_dag_id='1_kafka_to_bronze',          # Must match the dag_id inside your 2nd file
        wait_for_completion=True,
        poke_interval=60,
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id='trigger_dag_2',
        trigger_dag_id='2_bronze_to_silver',        # Must match the dag_id inside your 3rd file
        wait_for_completion=True,
        poke_interval=60,
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id='trigger_dag_3',
        trigger_dag_id='3_silver_to_gold',         # Must match the dag_id inside your 4th file
        wait_for_completion=True,
        poke_interval=60,
    )


    trigger_dag_0 >> trigger_dag_1 >> trigger_dag_2 >> trigger_dag_3
