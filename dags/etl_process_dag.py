from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
import os.path
import pandas as pd
import io
import boto3
from io import StringIO
import csv, re

from datetime import datetime
import datetime as dt

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
# from custom_modules.operator_s3_to_postgres import S3ToPostgresTransfer
from airflow.operators.python_operator import PythonOperator



class user_purchase():

    @apply_defaults
    def __init__(
        self,
        invoice_number,
        stock_code,
        detail,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country
    ):
        super(user_purchase, self).__init__()
        self.invoice_number = invoice_number
        self.stock_code = stock_code
        self.detail = detail
        self.quantity = quantity
        self.invoice_date = invoice_date
        self.unit_price = unit_price
        self.customer_id = customer_id
        self.country = country

class postgresql_to_s3_bucket(BaseOperator):

    template_fields = ()

    template_ext = ()

    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        schema,
        table,
        s3_bucket,
        s3_key,
        aws_conn_postgres_id="postgres_default",
        aws_conn_id="aws_default",
        verify=None,
        wildcard_match=False,
        copy_options=tuple(),
        autocommit=False,
        parameters=None,
        *args,
        **kwargs
    ):
        super(postgresql_to_s3_bucket, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id = aws_conn_postgres_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):

        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        s3_client = self.s3.get_conn()

        self.current_table = self.schema + "." + self.table

        self.pg_hook = PostgresHook(postgre_conn_id=self.aws_conn_postgres_id)

        fieldnames = ['invoice_number', 'stock_code', 'detail', 'quantity', 'invoice_date', 'unit_price', 'customer_id', 'country']

        queries_list = ["select * from dbname.user_purchase where invoice_date BETWEEN '2010-12-01 08:26:00'::timestamp and '2011-04-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-04-01 00:00:00'::timestamp and '2011-07-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-07-01 00:00:00'::timestamp and '2011-10-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-10-01 00:00:00'::timestamp and '2011-11-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-11-01 00:00:00'::timestamp and '2011-12-09 12:50:00'::timestamp"]
        outfileStr=""
        f = StringIO(outfileStr)
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for query in queries_list:
            
            self.request = (query)
            
            self.log.info(self.request)
            self.connection = self.pg_hook.get_conn()
            self.cursor = self.connection.cursor()
            self.cursor.execute(self.request)
            self.sources = self.cursor.fetchall()
            self.log.info(self.sources)

            for source in self.sources:
                obj = user_purchase(
                    invoice_number=source[0],
                    stock_code=source[1],
                    detail=source[2],
                    quantity=source[3],
                    invoice_date=source[4],
                    unit_price=source[5],
                    customer_id=source[6],
                    country=source[7]
                )
                w.writerow(vars(obj))
        s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=f.getvalue())


with DAG(
    "trigger_etl_processes",
    description="trigger distinct etl processes",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=70),
) as dag:

    trigger_glue_job_movies_reviews = AwsGlueJobOperator(
        job_name="etl-job-movies-reviews-",
        region_name = "us-east-2",
        iam_role_name="glue_role_paulirat",
        task_id="job",
        #script_location="s3://scripts-bucket-bootcamp-20230124233546053900000006/glue_script.py",
        #dag=dag2,
        #dag=dag4,
        script_args= {'--script_location': 's3://scripts-bucket-bootcamp-20230124233546053900000006/glue_script.py',
        '--example_movie_review_path':   's3://s3-data-bootcamp-20230124233546055000000007/movie_review.csv'}
        )

    trigger_glue_job_log_reviews = AwsGlueJobOperator(
        job_name="etl-job-log-reviews-",
        iam_role_name="glue_role_paulirat",
        task_id="job_log_reviews",
        #script_location="s3://scripts-bucket-bootcamp-20230124233546053900000006/glue_log_reviews.py",
        #dag=dag2,
        #dag=dag4,
        script_args= {'--log_review_xml_path':   's3://s3-data-bootcamp-20230124233546055000000007/review_log.xml',
        '--bucket_for_processed_data_path':   's3://processed-data-bucket-20230124233546055700000009/log_reviews',
        '--extra-jars':   's3://resources-bucket-20230124233546055400000008/spark-xml_2.11-0.4.0.jar',
        }
        )

    postgres_to_s3 = postgresql_to_s3_bucket(
        task_id="dag_postgres_to_s3",
        schema="dbname",  #'public'
        table="user_purchase",
        # s3_bucket="bucket-test-45",
        s3_bucket="processed-data-bucket-20230124233546055700000009",
        # s3_key="test_1.csv",
        s3_key="user_purchase_data_from_postgres.csv",
        aws_conn_postgres_id="postgres_default",
        aws_conn_id="aws_default",
        #dag=dag4
        )

    [trigger_glue_job_log_reviews, postgres_to_s3, trigger_glue_job_movies_reviews]
    #postgres_to_s3