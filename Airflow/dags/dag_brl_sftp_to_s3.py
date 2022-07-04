from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

# Redshift Variable
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

redshift_conn_id = 'data-redshift'

# AWS Glue Variable
source_bucket = 'captalys-analytics-land-{{var.value.aws_environment}}'
destination_bucket = 'captalys-analytics-raw-{{var.value.aws_environment}}'
land_bucket = 'captalys-analytics-land-{{var.value.aws_environment}}'
technology = 'sftp'
domain = 'brl'

relatorios = [
    'estoque',
    'liquidacao',
    'aquisicao']

prefix_download = ['liqbaix',
                   'estoquediario',
                   'aquisicoes']

tasks_glueJob_download_sftp_brl = []
tasks_redshift_insert_download_sftp_brl = []
tasks_glueJob_unzip_brl_to_csv = []
tasks_redshift_insert_unzip_brl_to_csv = []
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
# date = '{{ds}}'


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
        'username_client': 'datalake@captalys.com.br'
    }

    req.post(url, headers=headers, data=json.dumps(data))


with DAG(
        dag_id='Texte',
        schedule_interval='0 0 1 * *',
        start_date=datetime(2022, 6, 28),
        default_args=default_args,
        catchup=False,
        tags=['captalys', 'datalake', 'sftp', 'brl', 'unzip'],
        user_defined_macros={
            'get_date_execution_template': get_date_execution,
        }
        # on_failure_callback=notification,
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

    for prefix in prefix_download:
        tasks_glueJob_download_sftp_brl.append(
            AwsGlueJobOperator(
                task_id=f"tasks_glueJob_download_sftp_brl_{prefix}",
                pool='datalake_glueJob',
                job_name='datalake-sftp',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--host': 'sftran.brltrust.com.br',
                    '--username': 'captalys',
                    '--port': 22,
                    '--password': 'w@ferreira5',
                    '--bucket_name': 'captalys-analytics-land-production',
                    '--adm': 'brl',
                    '--path': 'estoque_diario',
                    '--day': '2022-06-28',
                    '--prefix': prefix,
                    '--str_key': 'brl/Key/sftp/v1/key-pem/acessoBRL.pem',
                    # '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',

                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )
        task_id = f"tasks_glueJob_download_sftp_brl_{prefix}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_download_sftp_brl.append(
            RedshiftSQLOperator(
                task_id=f'tasks_redshift_insert_download_sftp_brl_{prefix}',
                sql=f"""
                            INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                                '{task_id}',
                                '{start_date}',
                                '{end_date}'
                            );""",
                redshift_conn_id=redshift_conn_id
            )
        )

    for prefix in prefix_download:
        #for dests in relatorios:
            tasks_glueJob_unzip_brl_to_csv.append(
                AwsGlueJobOperator(
                    task_id=f"tasks_glueJob_unzip_brl_to_csv_{prefix}",
                    pool='datalake_glueJob',
                    job_name='unzip-file-adm',
                    script_location='s3://captalys-analyticsscripts-production/',
                    script_args={
                        '--bucket_name': 'captalys-analytics-land-production',
                        '--adm': 'brl',
                        '--data': '2022-06-28',
                        '--src': prefix,
                        #'--dest': dests
                        # '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    },
                    aws_conn_id='datalake-aws-analytics',
                    region_name='sa-east-1'
                )
            )

            task_id = f"tasks_glueJob_unzip_brl_to_csv_{prefix}"
            start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
            end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

            tasks_redshift_insert_unzip_brl_to_csv.append(
                RedshiftSQLOperator(
                    task_id=f'tasks_redshift_insert_unzip_brl_to_csv_{prefix}',
                    sql=f"""
                            INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                                '{task_id}',
                                '{start_date}',
                                '{end_date}'
                            );""",
                    redshift_conn_id=redshift_conn_id
                )
            )

    for files in relatorios:
        tasks_glueJob_raw.append(
            AwsGlueJobOperator(
                task_id=f"tasks_glueJob_raw_{files}",
                pool='datalake_glueJob',
                job_name='datalake-raw-brl',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--destination_bucket': destination_bucket,
                    '--domain': domain,
                    '--source_bucket': source_bucket,
                    '--technology': technology,
                    '--table': files
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )

        task_id = f"tasks_glueJob_raw_{files}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_glueJob_raw.append(
            RedshiftSQLOperator(
                task_id=f'tasks_redshift_insert_glueJob_raw_{files}',
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
                task_id=f"tasks_glueJob_trusted_{files}",
                pool='datalake_glueJob',
                job_name=' datalake-trusted-brl',
                script_location='s3://captalys-analyticsscripts-production/',
                script_args={
                    '--date': '{{ get_date_execution_template(ds, dag_run.conf.get("data_customizada"), True) }}',
                    '--domain': domain,
                    '--source_bucket': source_bucket,
                    '--technology': technology,
                    '--land_bucket': land_bucket,
                    '--file': files
                },
                aws_conn_id='datalake-aws-analytics',
                region_name='sa-east-1'
            )
        )
        task_id = f"tasks_glueJob_trusted_{files}"
        start_date = '''{{ dag_run.get_task_instance('%s').start_date}}''' % (task_id)
        end_date = '''{{ dag_run.get_task_instance('%s').end_date}}''' % (task_id)

        tasks_redshift_insert_glueJob_trusted.append(
            RedshiftSQLOperator(
                task_id=f'tasks_redshift_insert_glueJob_trusted_{files}',
                sql=f"""
                            INSERT INTO log.airflow_log (job_name, inicio, fim) VALUES (
                                '{task_id}',
                                '{start_date}',
                                '{end_date}'
                            );""",
                redshift_conn_id=redshift_conn_id
            )
        )

    tasks_glueJob_download_err = AwsGlueCrawlerOperator(
        task_id='tasks_glueJob_download_err',
        config={
            'Name': 'datalake-donwload-sftp-err'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    tasks_glueJob_unzip_err = AwsGlueCrawlerOperator(
        task_id='tasks_glueJob_unzip_err',
        config={
            'Name': 'datalake-unzip-sftp-err'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    tasks_glueJob_raw_err = AwsGlueCrawlerOperator(
        task_id='tasks_glueJob_raw_err',
        config={
            'Name': 'datalake-raw-sftp-err'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    tasks_glueJob_trusted_err = AwsGlueCrawlerOperator(
        task_id='tasks_glueJob_trusted_err',
        config={
            'Name': 'datalake-trusted-sftp-err'
        },
        aws_conn_id='datalake-aws-analytics'
    )

    (
            task_redshift_createTable
            >> [tasks_glueJob_download_sftp_brl[i]
                for i in range(len(tasks_glueJob_download_sftp_brl))]

            >> [tasks_glueJob_unzip_brl_to_csv[i]
                for i in range(len(tasks_glueJob_unzip_brl_to_csv))]

            >> [tasks_glueJob_raw[i] for i in range(len(tasks_glueJob_raw))]

            >> [tasks_glueJob_trusted[i]
                for i in range(len(tasks_glueJob_trusted))]

    )


    # (
    #         task_redshift_createTable
    #         >> [tasks_redshift_insert_download_sftp_brl[i] << tasks_glueJob_download_sftp_brl[i]
    #             for i in range(len(tasks_glueJob_download_sftp_brl))]
    #         >> tasks_glueJob_download_err
    #
    #         >> [tasks_redshift_insert_unzip_brl_to_csv[i] << tasks_glueJob_unzip_brl_to_csv[i]
    #             for i in range(len(tasks_glueJob_unzip_brl_to_csv))]
    #         >> tasks_glueJob_unzip_err
    #
    #         >> [tasks_redshift_insert_glueJob_raw[i] << tasks_glueJob_raw[i]
    #             for i in range(len(tasks_glueJob_raw))]
    #         >> tasks_glueJob_raw_err
    #
    #         >> [tasks_redshift_insert_glueJob_trusted[i] << tasks_glueJob_trusted[i]
    #             for i in range(len(tasks_glueJob_trusted))]
    #
    # )



