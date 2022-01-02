import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="create_schema_task",
    start_date=datetime.datetime(2021, 12, 31),
    schedule_interval=None,
    catchup=False,
) as dag:
    create_schema_deb = PostgresOperator(
        task_id="create_schema_deb_paulirat",
        postgres_conn_id = "terraform-20220102062020606600000010.cixft0af6zui.us-east-2.rds.amazonaws.com"
        sql="sql/user_purchase.sql",
    )

create_schema_deb