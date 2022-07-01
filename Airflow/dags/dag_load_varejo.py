from datetime import timedelta, datetime
from airflow import DAG
from airflow.models.variable import Variable

# Operators
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

from spark_script_varejo import spark_varejo


def script_dados_brutos():
    return spark_varejo


default_args = {
    "owner": "Captalys-Varejo",
    "depends_on_past": False,
    "retries": 3,
    "retries_delay": timedelta(minutes=20),
}

dag = DAG(
    'dag_varejo_dados_brutos',
    schedule_interval='0 10 * * *',
    start_date=datetime(2022, 5, 20),
    default_args=default_args,
    catchup=False,
    description='Atualizacao dos dados brutos relativos a Propostas e Pagamentos de opercoes Varejo',
)

t1 = PythonOperator(
    task_id='varejo_dados_brutos',
    python_callable=script_dados_brutos,
    op_kwargs={"x": "Apache Airflow"},
    dag=dag,
)

t1