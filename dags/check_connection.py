from airflow import settings
from airflow.models import Connection

def reset_and_create_postgres_connection(host, port, login, password, schema):
    session = settings.Session()

    # Resetting all existing connections with the given conn_id
    session.query(Connection).filter(Connection.conn_id == 'my_postgres_conn').delete()

    # Creating a new PostgreSQL connection
    my_postgres_conn = Connection(
        conn_id='my_postgres_conn',
        conn_type='postgres',
        host=host,
        port=port,
        login=login,
        password=password,
        schema=schema
    )

    session.add(my_postgres_conn)
    session.commit()

    print("PostgreSQL connection reset and created successfully.")
    return my_postgres_conn

my_postgres_conn = reset_and_create_postgres_connection(
    host='traffic-data-warehouse-postgres-1',
    port='5432',
    login='airflow',
    password='airflow',
    schema='traffic_data'
)
