from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.models import Connection
import pendulum
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from create_connetion import establish_connection


# Use the function to establisch a connection
my_postgres_conn = establish_connection('my_postgres_conn')

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

dag = DAG('load_data_to_postgres',
          default_args=default_args,
          schedule=None,
          )


def read_csv_to_df(file_path):
    return pd.read_csv(file_path)


def write_df_to_postgres(df, table_name):
   
    engine = create_engine(f'postgresql+psycopg2://{my_postgres_conn.login}:{my_postgres_conn.password}@{my_postgres_conn.host}:{my_postgres_conn.port}/{my_postgres_conn.schema}')

   
    df.to_sql(table_name, engine, if_exists='append', index=False)


def load_data_to_postgres():

    df_trajectory = read_csv_to_df('/tmp/data/trajectory_data.csv')
    df_vehicle_positions = read_csv_to_df('/tmp/data/vehicle_positions_data.csv')

    if df_trajectory.empty:
        print("No data in trajectory DataFrame.")
    else:
        print("Data is available in trajectory DataFrame.")
        print(df_trajectory.head())

    if df_vehicle_positions.empty:
        print("No data in vehicle_positions DataFrame.")
    else:
        print("Data is available in vehicle_positions DataFrame.")
        print(df_vehicle_positions.head())

    write_df_to_postgres(df_trajectory, 'trajectory_info')
    write_df_to_postgres(df_vehicle_positions, 'vehicle_positions')


load_trajectory_data = PythonOperator(
    task_id='load_trajectory_data',
    python_callable=load_data_to_postgres,
    dag=dag,
)

load_vehicle_positions_data = PythonOperator(
    task_id='load_vehicle_positions_data',
    python_callable=load_data_to_postgres,
    dag=dag,
)

load_trajectory_data >> load_vehicle_positions_data