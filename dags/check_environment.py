from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pkg_resources

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG('check_environment',
          default_args=default_args,
          schedule=None,
          )

def print_python_info():
    import sys
    print("Python version")
    print(sys.version)
    print("Version info.")
    print(sys.version_info)

def print_installed_packages():
    installed_packages = pkg_resources.working_set
    installed_packages_list = sorted(["%s==%s" % (i.key, i.version)
     for i in installed_packages])
    for m in installed_packages_list:
        print(m)

print_python_version = PythonOperator(
    task_id='print_python_version',
    python_callable=print_python_info,
    dag=dag,
)

print_packages = PythonOperator(
    task_id='print_packages',
    python_callable=print_installed_packages,
    dag=dag,
)

print_python_version >> print_packages