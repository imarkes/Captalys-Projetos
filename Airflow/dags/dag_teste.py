from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Ivan Marques',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='DAG-Estudos-01',
        default_args=default_args,
        description='Task para testar o bash operator',
        start_date=datetime(2022, 5, 12),
        schedule_interval='@daily'
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo SEJA BEM VINDO - IVAN'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo Hoje é um bom dia para aprender'
    )

    task1 >> task2
