from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from src.free_parking.extract_free_parking import extract_csv_free_parking

import os
import sys

project_root = os.path.abspath(os.curdir)
sys.path.append(os.path.join(project_root, "dags"))

with DAG("dags_infra_lyon",
         start_date=datetime(2022, 1, 1),
         schedule_interval="0 0 * * *",
         catchup=False
         ) as dag:

    start_el = DummyOperator(task_id='start_el', dag=dag)

    extract_csv_free_parking_task = PythonOperator(task_id="extract_csv_free_parking_task", python_callable=extract_csv_free_parking)

    end_el = DummyOperator(task_id='end_el', dag=dag)


    start_el >> extract_csv_free_parking_task >> end_el

