from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
#from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

# from airflow.providers.postgres.hooks.postgres.PostgresHook import PostgresHook
# from airflow.providers.amazon.aws.hooks.s3.S3Hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import pandas as pd
import io
import boto3
from io import StringIO
import csv, re



class S3ToPostgresTransfer(BaseOperator):
    """S3ToPostgresTransfer: custom operator created to move small csv files of data
                             to a postgresDB, it was created for DEMO.
       Author: Juan Escobar.
       Creation Date: 20/09/2022.

    Attributes:
    """

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

        # Validate if the file source exist or not in the bucket.
        # if self.wildcard_match:
        #     if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
        #         raise AirflowException("No key matches {0}".format(self.s3_key))
        #     s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
        # else:
        #     if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
        #         raise AirflowException(
        #             "The key {0} does not exists".format(self.s3_key)
        #        )

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
        df_user_purchase = pd.read_csv(
            io.StringIO(list_srt_content),
            header=0,
            delimiter=",",
            quotechar='"',
            low_memory=False,
            # parse_dates=date_cols,
            dtype=schema
        )
        self.log.info(df_user_purchase)
        self.log.info(df_user_purchase.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        #df_user_purchase.CustomerID.fillna(0, inplace = True)
        df_user_purchase.dropna(axis=0, subset = 'CustomerID', inplace = True)
        df_user_purchase = df_user_purchase.replace(r"[\"]", r"'")
        list_df_user_purchase = df_user_purchase.values.tolist()
        list_df_user_purchase = [tuple(user_purchase_str) for x in list_df_user_purchase]
        #list_df_user_purchase = list_df_user_purchase[621:623]
        self.log.info(list_df_user_purchase)

        # Read the file with the DDL SQL to create the table products in postgres DB.
        nombre_de_archivo = "dbname.user_purchase.sql"

        print("i reached here")

        print(os.path.sep)

        ruta_archivo = +os.path.sep + nombre_de_archivo
        # ruta_archivo = str(os.path.sep) + nombre_de_archivo
        #ruta_archivo = "/opt/airflow/dags/repo/dbname.products.sql"
        #ruta_archivo = "/Users/ana.rendon/airflow/dags/dbname.products.sql"


        self.log.info(ruta_archivo)
        proposito_del_archivo = "r"  # r es de Lectura
        codificación = "UTF-8"  # Tabla de Caracteres,
        # ISO-8859-1 codificación preferidad por
        # Microsoft, en Linux es UTF-8

        with open(
            ruta_archivo, proposito_del_archivo, encoding=codificación
        ) as manipulador_de_archivo:

            # Read dile with the DDL CREATE TABLE
            SQL_COMMAND_CREATE_TBL = manipulador_de_archivo.read()
            manipulador_de_archivo.close()

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
        # self.pg_hook.insert_rows(
        #     self.current_table,
        #     list_df_user_purchase,
        #     target_fields=list_target_fields,
        #     commit_every=1000,
        #     replace=False)
        with open('/Users/ana.rendon/airflow/dags/purchase_data_del.txt') as f:
            #f.write("\n".join(list_target_fields).encode("utf-8"))
            #f.flush()  
            self.pg_hook.bulk_load(self.current_table, f.name)

        # # Query and print the values of the table products in the console.
        # self.request = (
        #     "SELECT * FROM " + self.current_table + " WHERE stock_code = '85123A'"
        # )
        # self.log.info(self.request)
        # self.connection = self.pg_hook.get_conn()
        # self.cursor = self.connection.cursor()
        # self.cursor.execute(self.request)
        # self.sources = self.cursor.fetchall()
        # self.log.info(self.sources)

        # for source in self.sources:
        #     self.log.info(
        #         "producto: {0} - \
        #                    presentacion: {1} - \
        #                    marca: {2} - \
        #                    categoria: {3} - \
        #                    catalogo: {4} - \
        #                    precio: {5} - \
        #                    fechaRegistro: {6} - \
        #                    cadenaComercial: {7} - \
        #                    giro: {8} - \
        #                    nombreComercial: {9} - \
        #                    direccion: {10} - \
        #                    estado: {11} - \
        #                    municipio: {12} - \
        #                    latitud: {13} - \
        #                    longitud: {14} ".format(
        #             source[0],
        #             source[1],
        #             source[2],
        #             source[3],
        #             source[4],
        #             source[5],
        #             source[6],
        #             source[7],
        #             source[8],
        #             source[9],
        #             source[10],
        #             source[11],
        #             source[12],
        #             source[13],
        #             source[14],
        #         )
        #     )

# class GlueJobsTrigger(BaseOperator):     

#     @apply_defaults
#     def __init__(
#         self,
#         job_name,
#         iam_role_name,
#         concurrent_run_limit=1,
#         retry_limit=1,
#         num_of_dpus=1,
#         *args,
#         **kwargs
#     ):
#         super(S3ToPostgresTransfer, self).__init__(*args, **kwargs)
#         self.job_name = job_name
#         self.concurrent_run_limit = concurrent_run_limit
#         self.retry_limit = retry_limit
#         self.num_of_dpus = num_of_dpus
#         self.role_name = iam_role_name

#         def triggerGlueJob(self, context, **kwargs):
#             GlueJobHook.initialize_job(self.script_location, **kwargs)

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

        self.request = (
            "SELECT * FROM " + self.current_table
        )
        self.log.info(self.request)
        self.connection = self.pg_hook.get_conn()
        self.cursor = self.connection.cursor()
        self.cursor.execute(self.request)
        self.sources = self.cursor.fetchall()
        self.log.info(self.sources)

        fieldnames = ['invoice_number', 'stock_code', 'detail', 'quantity', 'invoice_date', 'unit_price', 'customer_id', 'country']
        outfileStr=""
        f = StringIO(outfileStr)
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
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
        # for y in self.sources:
        #     #w.writerow(vars(y))
        #     s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=f.getvalue())


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

        # # schema definition for data types of the source.
        # schema = {
        #         "log_id": "bigint",
        #         "log_date": "string",
        #         "device": "string",
        #         "os": "string",
        #         "location": "string",
        #         "browser": "string",
        #         "ip": "string",
        #         "phone_number": "string"
        # }

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

        self.s3.load_string(bucket_name=self.s3_bucket, key="review_logs_.xml", string_data=log_reviews_xml)

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

        user_purchase_df.dropna(axis=0, subset = 'CustomerID', inplace = True)

        user_purchase_df['Description'] = user_purchase_df['Description'].replace(r'\s\s+', '', regex=True)

        user_purchase_str = user_purchase_df.to_string(justify='justify-all',
                        col_space ={'InvoiceNo': 0 ,'StockCode': 10,'Description': 40,'Quantity': 5,'InvoiceDate': 20, 'UnitPrice': 5,'CustomerID': 10,'Country': 15 },
                        header=False,
                  index=False
                       )
        user_purchase_str = re.sub("   *" , '\t', user_purchase_str)

        user_purchase_str = re.sub("\t " , "\t", user_purchase_str)

        user_purchase_str = re.sub("\t\t+" , '\t', user_purchase_str)

        user_purchase_str = re.sub(" 536" , '536', user_purchase_str)

        self.s3.load_string(bucket_name=self.s3_bucket, key="user_purchase_tab_del.txt", string_data=user_purchase_str)



        #self.s3.load_file(bucket_name = self.s3_bucket, filename = "user_purchase_tab_del.txt", key = "user_purchase_tab_delimited.tx")



from datetime import datetime
import datetime as dt

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
# from custom_modules.operator_s3_to_postgres import S3ToPostgresTransfer
from airflow.operators.python_operator import PythonOperator


def print_welcome():
    return "Welcome from custom operator - Airflow DAG!"


dag1 = DAG(
    "dag_insert_data",
    description="Inser Data from CSV To Postgres",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=70),
)

dag2 = DAG(
    "dag_trigger_glue_job",
    description="triggers glue job for ETL",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=20),
)

dag3 = DAG(
    "dag_trigger_log_review_xml_creation",
    description="put into s3 bucket xml required",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=20),
)

dag4 = DAG(
    "dag_insert_data_from_postgres_to_s3",
    description="Inser Data from Postgres table to csv in s3 bucket",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=70),
)

dag5 = DAG(
    "dag_insert_tab_delimited_txt_into_s3_bucket",
    description="put into s3 bucket tab delimited text required",
    schedule_interval="@once",
    start_date=datetime(2021, 10, 1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=20),
)

welcome_operator = PythonOperator(
    task_id="welcome_task", python_callable=print_welcome, dag=dag1
)

s3_to_postgres_operator = S3ToPostgresTransfer(
    task_id="dag_s3_to_postgres",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="ss3-data-bootcamp-20220216024912394000000007",  
    # s3_key="test_1.csv",
    s3_key="user_purchase_data.csv",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    dag=dag1
)

from_s3_to_s3= fromS3toS3XML(
    task_id="from_s3_to_s3",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="s3-data-bootcamp-20220216024912394000000007",
    # s3_key="test_1.csv",
    s3_key="log_reviews_test.csv",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    dag=dag3
)

trigger_glue_job_movies_reviews = AwsGlueJobOperator(
    job_name="etl-job-movies-reviews-",
    iam_role_name="glue_role_paulirat",
    task_id="job",
    dag=dag2,
    script_args= {'--example_movie_review_path':   's3://s3-data-bootcamp-20220202174438981400000001/example_movies_reviews.csv',
    '--bucket_for_processed_data_path':   's3://processed-data-bucket-20220202174438981500000003/processed_data'}
        )

trigger_glue_job_log_reviews = AwsGlueJobOperator(
    job_name="etl-job-log-reviews-",
    iam_role_name="glue_role_paulirat",
    task_id="job_log_reviews",
    dag=dag2,
    script_args= {'--log_review_xml_path':   's3://s3-data-bootcamp-20220206043241941100000001/review_logs.xml',
    '--bucket_for_processed_data_path':   's3://processed-data-bucket-20220206043241941300000003/log_reviews/',
    '--extra-jars':   's3://jars-paulirat-deb/spark-xml_2.11-0.4.0.jar'}
        )
postgres_to_s3 = postgresql_to_s3_bucket(
    task_id="dag_postgres_to_s3",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="s3-data-bootcamp-20220216024912394000000007",
    # s3_key="test_1.csv",
    s3_key="user_purchase_data_from_postgres.csv",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    dag=dag4
)

from_s3_to_s3_tab_delimited= fromS3toS3TabDelimited(
    task_id="from_s3_to_s3_tab_del",
    schema="dbname",  #'public'
    table="user_purchase",
    # s3_bucket="bucket-test-45",
    s3_bucket="s3-data-bootcamp-20220216024912394000000007",
    # s3_key="test_1.csv",
    s3_key="user_purchase_data.csv",
    aws_conn_postgres_id="postgres_default",
    aws_conn_id="aws_default",
    dag=dag5
)

#s3_to_postgres_operator
#trigger_glue_job_movies_reviews
from_s3_to_s3
#trigger_glue_job_log_reviews
#ostgres_to_s3
#from_s3_to_s3_tab_delimited

# welcome_operator  # .set_downstream(s3_to_postgres_operator)
