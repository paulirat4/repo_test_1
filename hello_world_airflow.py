from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return "Hello world from first Airflow DAG!"


def print_welcome():
    return "Welcome from custom operator - Airflow DAG!"


dag = DAG(
    "hello_world",
    description="Hello World DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

welcome_operator = PythonOperator(
    task_id="welcome_task", python_callable=print_welcome, dag=dag
)

hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)

# hello_operator
welcome_operator