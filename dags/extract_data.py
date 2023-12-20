from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

dag = DAG('read_csv_data',
          default_args=default_args,
          schedule=None,
          )


def check_file(file_path):
    if os.path.isfile(file_path) and os.access(file_path, os.R_OK):
        print("File exists and is readable")
    else:
        print("Either the file is missing or not readable")


check_trajectory_file = PythonOperator(
    task_id='check_trajectory_file',
    python_callable=check_file,
    op_kwargs={'file_path': '/opt/airflow/dag/data/trajectory_data.csv'},
    dag=dag,
)

check_vehicle_positions_file = PythonOperator(
    task_id='check_vehicle_positions_file',
    python_callable=check_file,
    op_kwargs={'file_path': '/opt/airflow/dag/data/vehicle_positions_data.csv'},
    dag=dag,
)