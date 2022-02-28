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

class fromS3toS3TabDelimited(BaseOperator):
    
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
        super(fromS3toS3TabDelimited, self).__init__(*args, **kwargs)
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
        
        s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        # Read and decode the file into a list of strings.
        list_srt_content = (
            s3_key_object.get()["Body"].read().decode(encoding="utf-8", errors="ignore")
        )

        # schema definition for data types of the source.
        schema = {
            "invoice_number": "string",
            "stock_code": "string",
            "detail": "string",
            "quantity": "int",
            "invoice_date": "string",
            "unit_price": "float64",
            "customer_id": "int",
            "country": "string"
        }        

        # read a csv file with the properties required.
        user_purchase_df = pd.read_csv(
            io.StringIO(list_srt_content),
            header=0,
            delimiter=",",
            quotechar='"',
            low_memory=False,
            # parse_dates=date_cols,
            dtype=schema
        )

        # some transformations for converting into tab delimited txt
        user_purchase_df.dropna(inplace = True)
        user_purchase_df['CustomerID'] = user_purchase_df['CustomerID'].astype(int)

        user_purchase_df.to_csv(r'user_purchase_tab_del.txt', header=None, index=None, sep='	')

        self.s3.load_file(bucket_name = self.s3_bucket, filename = "user_purchase_tab_del.txt", key = "user_purchase_tab_delimited.txt", replace=True)


class S3ToPostgresTransfer(BaseOperator):

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
        super(S3ToPostgresTransfer, self).__init__(*args, **kwargs)
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

        self.log.info("Into the custom operator S3ToPostgresTransfer")

        # Create an instances to connect S3 and Postgres DB.
        self.log.info(self.aws_conn_postgres_id)

        self.pg_hook = PostgresHook(postgre_conn_id=self.aws_conn_postgres_id)
        self.log.info("Init PostgresHook..")    
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info("Downloading S3 file")
        self.log.info(self.s3_key + ", " + self.s3_bucket)


        s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        read_s3_object = self.s3.download_file(self.s3_key, self.s3_bucket)


        s3_key_obj_sql_file = self.s3.get_key("user_purchase_def.sql", self.s3_bucket)


        nombre_de_archivo = "dbname.user_purchase.sql"

        ruta_archivo = os.path.sep + nombre_de_archivo

        self.log.info(ruta_archivo)
        proposito_del_archivo = "r"  # r es de Lectura
        codificación = "UTF-8"  # Tabla de Caracteres,

        SQL_COMMAND_CREATE_TBL = self.s3.read_key("user_purchase_def.sql", self.s3_bucket)

            # Display the content
        self.log.info(SQL_COMMAND_CREATE_TBL)

        # execute command to create table in postgres.
        self.pg_hook.run(SQL_COMMAND_CREATE_TBL)

        # set the columns to insert, in this case we ignore the id, because is autogenerate.
        list_target_fields = [
            "invoice_number",
            "stock_code",
            "detail",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country"]

        self.current_table = self.schema + "." + self.table

        self.connection = self.pg_hook.get_conn()

        self.pg_hook.bulk_load(self.current_table, read_s3_object)

class fromS3toS3XML(BaseOperator):
    
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
        super(fromS3toS3XML, self).__init__(*args, **kwargs)
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
        
        s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        # Read and decode the file into a list of strings.
        list_srt_content = (
            s3_key_object.get()["Body"].read().decode(encoding="utf-8", errors="ignore")
        )

        # schema definition for data types of the source.
        schema = {
                "id_review": "int",
                "log": "string"
        }        

        # read a csv file with the properties required.
        log_reviews = pd.read_csv(
            io.StringIO(list_srt_content),
            header=0,
            delimiter=",",
            quotechar='"',
            low_memory=False,
            # parse_dates=date_cols,
            dtype=schema
        )
        # some transformations for converting into xml
        log_reviews.id_review = log_reviews.id_review.astype(str)
        log_reviews.log = log_reviews.log.apply(lambda x: x[11:-12])
        log_reviews['log_part1'] = log_reviews.log.apply(lambda x: x[:5] + "<log_id>")
        log_reviews['log_part2'] = log_reviews.log.apply(lambda x:  "</log_id>" + x[5:])
        log_reviews['complete_log'] =  log_reviews['log_part1'] + log_reviews['id_review'] + log_reviews['log_part2']
        log_reviews_xml = ""
        for review in log_reviews.iloc[:, 4]:
            log_reviews_xml = log_reviews_xml + review
        log_reviews_xml = '<reviewlog>' + log_reviews_xml + '</reviewlog>'

        self.s3.load_string(bucket_name=self.s3_bucket, key="review_log.xml", string_data=log_reviews_xml, replace=True)
       
  

with DAG(
    "dag_insert_data",
    description="Inser Data from CSV To Postgres",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=70),
) as dag:

    s3_to_postgres_operator = S3ToPostgresTransfer(
    task_id="dag_s3_to_postgres",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="s3-data-bootcamp-20220227204648095000000005",  
    # s3_key="test_1.csv",
    s3_key="user_purchase_tab_delimited.txt",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    #dag=dag1
    )

    from_s3_to_s3= fromS3toS3XML(
    task_id="from_s3_to_s3",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="s3-data-bootcamp-20220227204648095000000005",
    # s3_key="test_1.csv",
    s3_key="log_reviews_test.csv",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    #dag=dag3
    )

    from_s3_to_s3_tab_delimited= fromS3toS3TabDelimited(
    task_id="from_s3_to_s3_tab_del",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="s3-data-bootcamp-20220227204648095000000005",
    # s3_key="test_1.csv",
    s3_key="user_purchase_data.csv",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    #dag=dag1
    )

    [from_s3_to_s3_tab_delimited, from_s3_to_s3] >> s3_to_postgres_operator


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
            #"SELECT * FROM " + self.current_table + " LIMIT 100000")
            
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

    # trigger_glue_job_movies_reviews = AwsGlueJobOperator(
    #     job_name="etl-job-movies-reviews-",
    #     region_name = "us-east-2",
    #     iam_role_name="glue_role_paulirat",
    #     task_id="job",
    #     #dag=dag2,
    #     #dag=dag4,
    #     script_args= {'--example_movie_review_path':   's3://s3-data-bootcamp-20220227204648095000000005/example_movies_reviews.csv',
    #     '--bucket_for_processed_data_path':   's3://processed-data-bucket-20220227204648097100000008/movie_reviews'}
    #     )

    # trigger_glue_job_log_reviews = AwsGlueJobOperator(
    #     job_name="etl-job-log-reviews-",
    #     iam_role_name="glue_role_paulirat",
    #     task_id="job_log_reviews",
    #     #dag=dag2,
    #     #dag=dag4,
    #     script_args= {'--log_review_xml_path':   's3://s3-data-bootcamp-20220227204648095000000005/review_log.xml',
    #     '--bucket_for_processed_data_path':   's3://processed-data-bucket-20220227204648097100000008/log_reviews',
    #     '--extra-jars':   's3://resources-bucket-20220227204648095100000006/spark-xml_2.11-0.4.0.jar'}
    #     )
    postgres_to_s3 = postgresql_to_s3_bucket(
        task_id="dag_postgres_to_s3",
        schema="dbname",  #'public'
        table="user_purchase",
        # s3_bucket="bucket-test-45",
        s3_bucket="s3-data-bootcamp-20220227204648095000000005",
        # s3_key="test_1.csv",
        s3_key="user_purchase_data_from_postgres.csv",
        aws_conn_postgres_id="postgres_default",
        aws_conn_id="aws_default",
        #dag=dag4
        )

    #[trigger_glue_job_log_reviews, postgres_to_s3, trigger_glue_job_movies_reviews]
    postgres_to_s3