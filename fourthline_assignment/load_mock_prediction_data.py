import logging
import os
import random
import datetime
import json
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import glob
import sqlite3

START_DATE = datetime.datetime(2022, 8, 12)
dag = DAG(
    dag_id='data_load_prediction_dag',
    start_date=START_DATE,
    schedule_interval='0 9 * * *',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False
)

dag.doc_md = """\
This dag simply reads all request files and append new field probability_of_fraud to the record and create a new file in prediction directory.
Then loads prediction data and decision data to sqlite database.
"""

def load_prediction_data():
    connection = sqlite3.connect(f"{os.path.expanduser('~')}/fourthline.db")
    logging.info("connect to database fourthline.db")
    cursor = connection.cursor()
    cursor.execute(f'Select * from request')
    data = cursor.fetchall()
    cursor.execute(f'Drop Table if exists prediction')
    connection.commit()
    cursor.execute(f'Create Table if not exists prediction (raw json)')
    connection.commit()
    for i in data:
        jd = json.loads(i[0])
        jd['probability_of_fraud'] = random.random()
        cursor.execute('insert into prediction values(?)', [json.dumps(jd)])
        connection.commit()
    logging.info("load prediction mockup data to sqlite")
    connection.close()

START_DAG = LatestOnlyOperator(task_id='latest_only', dag=dag)
load_mock_prediction = PythonOperator(
    task_id='load_mock_prediction',
    python_callable=load_prediction_data,
    provide_context=True,
    dag=dag)


START_DAG >> load_mock_prediction