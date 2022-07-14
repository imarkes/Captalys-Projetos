from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.dummy_operator import DummyOperator
# Redshift Variable
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

redshift_conn_id = 'data-redshift'

# AWS Glue Variable
source_bucket = 'captalys-analytics-land-{{var.value.aws_environment}}'
destination_bucket = 'captalys-analytics-raw-{{var.value.aws_environment}}'
land_bucket = 'captalys-analytics-land-{{var.value.aws_environment}}'
localhost = '{{var.value.localhost}}'
private_key = '{{var.value.private_key}}'
username = '{{var.value.username}}'
technology = 'sftp'
folder = 'estoque'
domain = 'singulare'
qt_days_reprocessing = '10'

files = [
    'estoque', 'liquidacao', 'aquisicao'
]

tasks_glueJob_download_sftp_singulare = []
tasks_glueJob_raw = []
tasks_redshift_insert_glueJob_raw = []
tasks_glueJob_trusted = []
tasks_redshift_insert_glueJob_trusted = []


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
#date = '{{ds}}'


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
        'subject': 'Error on DAG sftp-BRL-Files in client Captalys',
        'error_msg': 'Error on DAG sftp-BRL-Files',
        'username_client': 'xxdatalake@captalys.com.br'
    }

    req.post(url, headers=headers, data=json.dumps(data))


with DAG(
        dag_id='datalake_v1_load_singulare',
        schedule_interval='1 * * * *',
        start_date=datetime(2022, 6, 26),
        default_args=default_args,
        catchup=False,
        tags=['captalys', 'datalake', 'sftp', 'singulare'],
        user_defined_macros={
            'get_date_execution_template': get_date_execution
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

    for file in files:
        tasks_glueJob_download_sftp_singulare.append(
            AwsGlueJobOperator(
                task_id=f"tasks_glueJob_download_sftp_singulare_{file}",
                pool='datalake_glueJob',
                job_name='datalake-sftp-down-singulare',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--host': localhost,
                    '--username': username,
                    '--port': 22,
                    '--bucket_name': land_bucket,
                    '--domain': domain,
                    '--folder': folder,
                    '--docs': file,
                    '--private_key': private_key,
                    '--qt_days_reprocessing': qt_days_reprocessing
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )
        task_id = f"glueJob_sftp-down-singulare_{file}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_glueJob_raw.append(
            AwsGlueJobOperator(
                task_id=f"glueJob_raw_{file}",
                pool='datalake_glueJob',
                job_name='ddatalake-land_to_raw-singulare',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--domain': domain,
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--destination_bucket': destination_bucket,
                    '--file': file,
                    '--source_bucket': source_bucket,
                    '--technology': technology,
                    '--qt_days_reprocessing': qt_days_reprocessing
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
                job_name='datalake-trusted-singulare',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--domain': domain,
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--file': file,
                    '--land_bucket': land_bucket,
                    '--source_bucket': source_bucket,
                    '--technology': technology,
                    '--qt_days_reprocessing':qt_days_reprocessing
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

    task_glueCrawler_raw_singulare = AwsGlueCrawlerOperator(
        task_id='glueCrawler_raw_singulare',
        config={
            'Name': 'datalake-raw-singulare'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    task_redshift_insert_glueCrawler_raw_singulare = RedshiftSQLOperator(
        task_id='redshift_insert_glueCrawler_raw_singulare',
        sql="""
                    INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                        'glueCrawler_raw_singulare',
                        '{{ dag_run.get_task_instance('glueCrawler_raw_singulare').start_date }}',
                        '{{ dag_run.get_task_instance('glueCrawler_raw_singulare').end_date }}'
                    );
                """,
        redshift_conn_id=redshift_conn_id
    )

    dummySeq01 = DummyOperator(
        task_id='dummySeq01'
    )

    (
            task_redshift_createTable
            >> [tasks_glueJob_download_sftp_singulare[i] for i in range(len(tasks_glueJob_download_sftp_singulare))]
            >> dummySeq01
            >> [tasks_redshift_insert_glueJob_raw[i] << tasks_glueJob_raw[i] for i in range(len(tasks_glueJob_raw))]
            >> task_glueCrawler_raw_singulare
            >> [tasks_redshift_insert_glueJob_trusted[i] << tasks_glueJob_trusted[i] for i in
                range(len(tasks_glueJob_trusted))]

    )
    task_glueCrawler_raw_singulare >> task_redshift_insert_glueCrawler_raw_singulare

