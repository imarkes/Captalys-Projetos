from datetime import datetime, timedelta

# The DAG object
from airflow import DAG
from airflow.models.variable import Variable

# Operators
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.dummy_operator import DummyOperator

# Redshift Variable
redshift_conn_id = 'data-redshift'

# AWS Glue Variable
land_bucket = 'captalys-analytics-land-{{var.value.aws_environment}}'
raw_bucket = 'captalys-analytics-raw-{{var.value.aws_environment}}'
source_bucket = 'captalys-analytics-adms-{{var.value.aws_environment}}'
domain = 'brl'
technology = 'sftp'
qt_days_reprocessing = '3'

file_load = [
    'estoque',
    'liquidacao',
    'aquisicao'
]


def get_date_execution(str_date_logical, str_date_params, check_date_atual):
    if str_date_params is not None:
        return str_date_params
    else:
        date_logical = datetime.strptime(str_date_logical, "%Y-%m-%d").date()
        if check_date_atual and date_logical >= datetime.today().date():
            t_days = timedelta(days=1)
            sub_date_logical = date_logical - t_days
            return sub_date_logical.strftime("%Y-%m-%d")
        else:
            return str_date_logical


# "{{ get_date_execution_template(ds, dag_run.conf.get('data_customizada'), True) }}"
# date = '{{ds}}'

tasks_glueJob_raw = []
tasks_redshift_insert_glueJob_raw = []
tasks_glueJob_trusted = []
tasks_redshift_insert_glueJob_trusted = []
tasks_glueJob_download_sftp = []
tasks_glueJob_unzip_file = []

default_args = {
    'owner': 'Ivan Marques',
    'email': ['alertas.datalake@captalys.com.br'],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=30)
}


def notification(context):
    import requests as req
    import json

    url = 'https://us-central1-roxpartner-core.cloudfunctions.net/open_ticket_movidesk'

    headers = {'Content-Type': 'application/json'}

    data = {
        'subject': 'Error on DAG datalake_load_dms_cci in client Captalys',
        'error_msg': 'Error on DAG datalake_load_dms_cci',
        'username_client': 'xdatalake@captalys.com.br'
    }

    req.post(url, headers=headers, data=json.dumps(data))


with DAG(
        dag_id='Datalake_v2_load_sftp_brl',
        schedule_interval='1 * * * *',
        start_date=datetime(2022, 7, 14),
        default_args=default_args,
        catchup=False,
        on_failure_callback=notification,
        tags=['captalys', 'datalake', 'sftp', 'brl', 'unzip'],
        user_defined_macros={
            'get_date_execution_template': get_date_execution,
        }
) as dag:
    task_redshift_createTable = RedshiftSQLOperator(
        task_id='redshift_createTable',
        sql="""
            CREATE TABLE IF NOT EXISTS log.airflow_log (
                id BIGINT IDENTITY(0, 1),
                job_name VARCHAR NOT NULL,
                inicio DATETIME NOT NULL,
                fim DATETIME
            );
        """,
        redshift_conn_id=redshift_conn_id
    )

    for file in file_load:
        # Download
        tasks_glueJob_download_sftp.append(
            AwsGlueJobOperator(
                task_id=f"glueJob_download_sftp_{file}",
                pool='datalake_glueJob',
                job_name='datalake-sftp-down-brl',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--dest_bucket': land_bucket,
                    '--source_bucket': source_bucket,
                    '--domain': domain,
                    '--tecnology': technology,
                    '--qt_days_reprocessing': qt_days_reprocessing,
                    '--file': file,
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )
        task_id = f"glueJob_download_sftp_{file}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        # Unzip file to csv
        tasks_glueJob_unzip_file.append(
            AwsGlueJobOperator(
                task_id=f"glueJob_unzip_file_{file}",
                pool='datalake_glueJob',
                job_name='datalake-unzip-brl',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--bucket_name': land_bucket,
                    '--domain': domain,
                    '--file': file,
                    '--qt_days_reprocessing': qt_days_reprocessing,
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )
        task_id = f"glueJob_unzip_file_{file}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_glueJob_raw.append(
            AwsGlueJobOperator(
                task_id=f"glueJob_raw_{file}",
                pool='datalake_glueJob',
                job_name='datalake-raw-brl',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--domain': domain,
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--destination_bucket': raw_bucket,
                    '--file': file,
                    '--source_bucket': land_bucket,
                    '--technology': technology,
                    '--qt_days_reprocessing': qt_days_reprocessing,
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )

        task_id = f"glueJob_raw_{file}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_glueJob_raw.append(
            RedshiftSQLOperator(
                task_id=f'redshift_insert_glueJob_raw_{file}',
                sql=f"""
                    INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                        '{task_id}',
                        '{start_date}',
                        '{end_date}'
                    );""",
                redshift_conn_id=redshift_conn_id
            )
        )

        tasks_glueJob_trusted.append(
            AwsGlueJobOperator(
                task_id=f"glueJob_trusted_{file}",
                pool='datalake_glueJob',
                job_name='datalake-trusted-brl',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--domain': domain,
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--file': file,
                    '--land_bucket': land_bucket,
                    '--source_bucket': raw_bucket,
                    '--technology': technology,
                    '--qt_days_reprocessing': qt_days_reprocessing,
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )

        task_id = f"glueJob_trusted_{file}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_glueJob_trusted.append(
            RedshiftSQLOperator(
                task_id=f'redshift_insert_glueJob_trusted_{file}',
                sql=f"""
                    INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                        '{task_id}',
                        '{start_date}',
                        '{end_date}'
                    );""",
                redshift_conn_id=redshift_conn_id
            )
        )

    task_glueCrawler_raw_brl = AwsGlueCrawlerOperator(
        task_id='glueCrawler_raw_brl',
        config={
            'Name': 'datalake-raw-brl'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    task_redshift_insert_glueCrawler_raw_brl = RedshiftSQLOperator(
        task_id='redshift_insert_glueCrawler_raw_brl',
        sql="""
            INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                'glueCrawler_raw_brl',
                '{{ dag_run.get_task_instance('glueCrawler_raw_brl').start_date }}',
                '{{ dag_run.get_task_instance('glueCrawler_raw_brl').end_date }}'
            );
        """,
        redshift_conn_id=redshift_conn_id
    )

    dummySeq01 = DummyOperator(
        task_id='dummySeq01'
    )

    dummySeq02 = DummyOperator(
        task_id='dummySeq02'
    )

    (
            task_redshift_createTable
            >> [tasks_glueJob_download_sftp[i] for i in range(len(tasks_glueJob_download_sftp))]
            >> dummySeq01
            >> [tasks_glueJob_unzip_file[i] for i in range(len(tasks_glueJob_unzip_file))]
            >> dummySeq02
            >> [tasks_redshift_insert_glueJob_raw[i] << tasks_glueJob_raw[i] for i in range(len(tasks_glueJob_raw))]
            >> task_glueCrawler_raw_brl
            >> [tasks_redshift_insert_glueJob_trusted[i] << tasks_glueJob_trusted[i] for i in
                range(len(tasks_glueJob_trusted))]
    )

    task_glueCrawler_raw_brl >> task_redshift_insert_glueCrawler_raw_brl
