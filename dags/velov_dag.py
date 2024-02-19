from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


from src.station_status.extract_station_status import extract_json_station_status
from src.station_information.extract_station_information import extract_json_station_information
from src.station_status.load_station_status import load_bq_station_status
from src.station_information.load_station_information import load_bq_station_information

import os
import sys

project_root = os.path.abspath(os.curdir)
sys.path.append(os.path.join(project_root, "dags"))

with DAG("dags_velov_lyon",
         start_date=datetime(2022, 1, 1),
         schedule_interval="@hourly",
         catchup=False
         ) as dag:

    start_el = DummyOperator(task_id='start_el', dag=dag)

    extract_station_status_task = PythonOperator(task_id="extract_station_status_task", python_callable=extract_json_station_status)
    extract_station_information_task = PythonOperator(task_id="extract_station_information_task", python_callable=extract_json_station_information)

    load_station_status_task = PythonOperator(task_id="load_station_status_task", python_callable=load_bq_station_status)
    load_station_information_task = PythonOperator(task_id="compute_colomns_task", python_callable=load_bq_station_information)

    end_el = DummyOperator(task_id='end_el', dag=dag)


    start_el >> extract_station_status_task >> load_station_status_task >> end_el
    start_el >> extract_station_information_task >> load_station_information_task >> end_el


