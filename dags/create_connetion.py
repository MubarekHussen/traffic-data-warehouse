from airflow import settings
from airflow.models import Connection
from airflow.operators.python import PythonOperator


def establish_connection(conn_id):
    my_postgres_conn = (
        settings.Session()
        .query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first()
    )

    if my_postgres_conn:
        print(f"Connection established with: {my_postgres_conn.conn_id}")
        return my_postgres_conn
    else:
        print("Connection not established.")
        return None
