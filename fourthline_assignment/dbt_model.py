from airflow import DAG
import datetime
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.bash import BashOperator
import os
import sqlite3
import logging
from airflow.operators.python import PythonOperator

START_DATE = datetime.datetime(2022, 8, 13)

dag = DAG(
    dag_id='fourthline_dbt_model_dag',
    start_date=START_DATE,
    schedule_interval='0 8 * * *',
    max_active_runs=1,
    catchup=False
)

dag.doc_md = """\
This Dag is used for update dbt models.
"""

def performnce():
    connection = sqlite3.connect(f"{os.path.expanduser('~')}/fourthline.db")
    logging.info("connect to database fourthline.db")
    cursor = connection.cursor()
    cursor.execute(f'Select count(*) as total_records from performance')
    all_records = cursor.fetchall()[0][0]
    cursor.execute(f'Select count(*) as total_records from performance where final_decision_by_agent = prediction_decision')
    all_correct_records = cursor.fetchall()[0][0]
    logging.info(f"performce is {all_correct_records*1.0/all_records}")

DBT_DIR = f"{os.path.expanduser('~')}/airflow/dags/fourthline_dbt_model/"

START_DAG = LatestOnlyOperator(task_id='latest_only', dag=dag)
model_update = BashOperator(
    task_id='update_models',
    bash_command=f"""
            cd {DBT_DIR} &&
            dbt run
            """,
    dag=dag
)

calculate_performance = PythonOperator(
    task_id='calculate_performance',
    python_callable=performnce,
    provide_context=True,
    dag=dag)

START_DAG >> model_update >> calculate_performance