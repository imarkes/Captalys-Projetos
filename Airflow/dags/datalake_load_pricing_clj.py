from datetime import datetime, timedelta

# The DAG object
from airflow import DAG
from airflow.models.variable import Variable

# Operators
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

# Redshift Variable
redshift_conn_id = 'data-redshift'

# AWS Glue Variable
source_bucket_v1 = 'captalys-analytics-land-{{var.value.aws_environment}}'

destination_bucket = 'captalys-analyticsrecebiveis-raw-{{var.value.aws_environment}}'

"""
    Dag desenvolvida para novas tabelas
"""

domain = 'recebiveis'
database = 'pricing_clj'
schema = 'master'
instancia = 'geral'
tables = [
    'offer',
    'credit',
]
date = '{{ds}}'

tasks_glueJob_raw = []
tasks_redshift_insert_glueJob_raw = []
tasks_glueJob_trusted = []
tasks_redshift_insert_glueJob_trusted = []

default_args = {
    'owner': 'roxpartner',
    'email': ['servicedesk@roxpartner.com'],
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
        'subject': 'Error on DAG datalake_load_pricing_clj in client Captalys',
        'error_msg': 'Error on DAG datalake_pricing_clj',
        'username_client': 'datalake@captalys.com.br'
    }

    req.post(url, headers=headers, data=json.dumps(data))


with DAG(
        dag_id='datalake_load_pricing_clj',
        schedule_interval='0 8 * * *',
        start_date=datetime(2022, 5, 16),
        default_args=default_args,
        catchup=False,
        on_failure_callback=notification,
        tags=['roxpartner', 'datalake', 'recebiveis', 'pricing_clj']
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

    for table in tables:
        tasks_glueJob_raw.append(
            AwsGlueJobOperator(
                task_id=f"glueJob_raw_{table}",
                pool='datalake_glueJob',
                job_name='datalake-raw-recebiveis',
                script_args={
                    '--database': database,
                    '--domain': domain,
                    '--source_bucket': source_bucket_v1,
                    '--table': table,
                    '--date': date,
                    '--destination_bucket': destination_bucket,
                    '--schema': schema,
                    '--instancia': instancia
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )

        task_id = f"glueJob_raw_{table}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_glueJob_raw.append(
            RedshiftSQLOperator(
                task_id=f'redshift_insert_glueJob_raw_{table}',
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
                task_id=f"glueJob_trusted_{table}",
                pool='datalake_glueJob',
                job_name='datalake-trusted',
                script_args={
                    '--database': database,
                    '--domain': domain,
                    '--full_load': '1',
                    '--source_bucket': source_bucket_v1,
                    '--table': table,
                    '--date': date,
                    '--schema': schema
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )

        task_id = f"glueJob_trusted_{table}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_glueJob_trusted.append(
            RedshiftSQLOperator(
                task_id=f'redshift_insert_glueJob_trusted_{table}',
                sql=f"""
                    INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                        '{task_id}',
                        '{start_date}',
                        '{end_date}'
                    );""",
                redshift_conn_id=redshift_conn_id
            )
        )

    task_glueCrawler_raw_pricing_clj = AwsGlueCrawlerOperator(
        task_id='glueCrawler_raw_pricing_clj',
        config={
            'Name': 'datalake-raw-pricing_clj'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    task_redshift_insert_glueCrawler_raw_pricing_clj = RedshiftSQLOperator(
        task_id='redshift_insert_glueCrawler_raw_pricing_clj',
        sql="""
            INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES ( 
                'glueCrawler_raw_pricing_clj', 
                '{{ dag_run.get_task_instance('glueCrawler_raw_pricing_clj').start_date }}', 
                '{{ dag_run.get_task_instance('glueCrawler_raw_pricing_clj').end_date }}'
            );
        """,
        redshift_conn_id=redshift_conn_id
    )

    task_glueCrawler_raw_pricing_clj_err = AwsGlueCrawlerOperator(
        task_id='glueCrawler_raw_pricing_clj_err',
        config={
            'Name': 'datalake-raw-pricing_clj-err'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    task_redshift_insert_glueCrawler_raw_pricing_clj_err = RedshiftSQLOperator(
        task_id='redshift_insert_glueCrawler_raw_pricing_clj_err',
        sql="""
            INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES ( 
                'glueCrawler_raw_pricing_clj_err', 
                '{{ dag_run.get_task_instance('glueCrawler_raw_pricing_clj_err').start_date }}', 
                '{{ dag_run.get_task_instance('glueCrawler_raw_pricing_clj_err').end_date }}'
            );
        """,
        redshift_conn_id=redshift_conn_id
    )

    (
            task_redshift_createTable
            >> [tasks_redshift_insert_glueJob_raw[i] << tasks_glueJob_raw[i] for i in range(len(tasks_glueJob_raw))]
            >> task_glueCrawler_raw_pricing_clj
            >> [tasks_redshift_insert_glueJob_trusted[i] << tasks_glueJob_trusted[i] for i in
                range(len(tasks_glueJob_trusted))]
    )

    task_glueCrawler_raw_pricing_clj >> task_glueCrawler_raw_pricing_clj_err
    task_glueCrawler_raw_pricing_clj >> task_redshift_insert_glueCrawler_raw_pricing_clj
    task_glueCrawler_raw_pricing_clj_err >> task_redshift_insert_glueCrawler_raw_pricing_clj_err
