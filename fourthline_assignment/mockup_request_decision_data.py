import logging
import os
from tempfile import TemporaryDirectory
from datetime import timedelta
import random
import datetime
import string
import json
import sqlite3
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
START_DATE = datetime.datetime(2022, 8, 12)
dag = DAG(
    dag_id='data_mockup_decision_request_dag',
    start_date=START_DATE,
    schedule_interval='0 7 * * *',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False
)

dag.doc_md = """\
This dag creates mockup data for last 30 days into a tmp folder each time when the dag runs, data is loaded to sqlite and tmp folder will be automatically here after a short period. 
In the temp folder, path of each hour of the day is creted, then generates random number of records (no more than 100) of requests and writes the records to request_X.json file. 
At the same time a decision_X.json is also created with generated records following same method.
"""

def data_mock():
    connection = sqlite3.connect(f"{os.path.expanduser('~')}/fourthline.db")
    logging.info("connect to database fourthline.db")
    cursor = connection.cursor()

    # drop the table if exists
    cursor.execute(f'Drop Table if exists request')
    connection.commit()
    cursor.execute(f'Create Table if not exists request (raw json)')
    connection.commit()

    # load decision mockup data to sqlite
    cursor.execute(f'Drop Table if exists decision')
    connection.commit()
    cursor.execute(f'Create Table if not exists decision (raw json)')
    connection.commit()

    #create temp folder for mockup data
    with TemporaryDirectory("w") as tmp:
        # create mockup data for last 30 days
        for i in range(1,30):
            request_date = datetime.datetime.now().date() - datetime.timedelta(days=i)
            #create path for each hour
            for h in range(0,24):
                request_hour = datetime.datetime.combine(request_date, datetime.datetime.min.time()) + timedelta(hours=h)
                request_path = f"{tmp}/request/year={request_hour.strftime('%Y')}/month={request_hour.strftime('%m')}/day={request_hour.strftime('%d')}/hour={request_hour.strftime('%H')}"
                decision_path = f"{tmp}/decision/year={request_hour.strftime('%Y')}/month={request_hour.strftime('%m')}/day={request_hour.strftime('%d')}/hour={request_hour.strftime('%H')}"
                #create path for both mockup request data and mockup decision data
                os.makedirs(request_path)
                logging.info(f"path {request_path} is created")
                os.makedirs(decision_path)
                logging.info(f"path {decision_path} is created")
                data_request_list = []
                data_decision_list = []
                #number of data records from requests of each hour is random within (0, 100)
                for s in range(random.randint(0, 100)):
                    data_request = {}
                    data_decision = {}
                    request_timestamp = str((request_hour + timedelta(seconds=random.randint(0, 60*60-1))).strftime("%m/%d/%Y, %H:%M:%S"))
                    chars = list(set(string.ascii_uppercase + string.digits + string.ascii_lowercase))
                    digits = list(set(string.digits))
                    request_id = ''.join(random.choices(chars, k=20))
                    data_request['request_id'] = request_id
                    data_decision['request_id'] = request_id
                    data_decision['agent_id'] = ''.join(random.choices(digits, k=20))
                    data_request['request_timestamp'] = request_timestamp
                    data_decision['timestamp'] = request_timestamp
                    data_request['latitude'] = random.uniform(-180,180)
                    data_request['longitude'] = random.uniform(-90,90)
                    percentage_list = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
                    data_request['document_photo_brightness_percent'] = random.choices(percentage_list)[0]
                    data_request['is_photo_in_a_photo_selfie'] = random.randint(0, 1)
                    data_decision['is_fraud_request'] = random.randint(0, 1)
                    data_request['document_matches_selfie_percent'] = random.choices(percentage_list)[0]
                    data_request['s3_path_to_selfie_photo'] = f"{request_path}/selfie_photo/{request_id}.jpg"
                    data_request['s3_path_to_document_photo'] = f"{request_path}/document_photo/{request_id}.jpg"
                    data_request['metadata'] = request_path
                    data_decision['metadata'] = decision_path
                    data_request_list = data_request_list + [data_request]
                    data_decision_list = data_decision_list + [data_decision]
                #mockup data is created temporarily in tmp
                with open(f'{request_path}/request_X.json', 'w+') as f:
                    result = [json.dumps(record) for record in data_request_list]
                    f.write('\n'.join(result))
                    f.close()
                    logging.info(f"file {request_path}/request_X.json is created temporarily")

                    for i in result:
                        cursor.execute('insert into request values(?)', [i])
                        connection.commit()
                    logging.info("load request mockup data to sqlite")
                with open(f'{decision_path}/decision_X.json', 'w+') as f:
                    result = [json.dumps(record) for record in data_decision_list]
                    f.write('\n'.join(result))
                    f.close()
                    logging.info(f"file {decision_path}/decision_X.json is created temporarily")

                    for i in result:
                        cursor.execute('insert into decision values(?)', [i])
                        connection.commit()
                    logging.info("load decision mockup data to sqlite")

START_DAG = LatestOnlyOperator(task_id='latest_only', dag=dag)
mock_request_decision_data = PythonOperator(
    task_id='mock_request_decision_data',
    python_callable=data_mock,
    provide_context=True,
    op_kwargs={
        'root': 'dataset'
    },
    dag=dag)

START_DAG >> mock_request_decision_data