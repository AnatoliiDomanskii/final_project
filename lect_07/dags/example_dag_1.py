from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 60,
}


dag = DAG(
    dag_id='example_dag_1',
    start_date=datetime(2022, 9, 16),
    schedule_interval="0 5 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)


start = EmptyOperator(
    task_id='start',
    dag=dag
)

task1 = BashOperator(
    task_id='task1',
    dag=dag,
    bash_command='echo hello world!',
)

task2 = BashOperator(
    task_id='task2',
    dag=dag,
    bash_command='echo hello world!',
)

task3 = BashOperator(
    task_id='task3',
    dag=dag,
    bash_command='sleep foo',
)

task4 = BashOperator(
    task_id='task4',
    dag=dag,
    bash_command='echo hello world!',
)

task5 = BashOperator(
    task_id='task5',
    dag=dag,
    bash_command='echo hello world!',
)

start >> [task1, task2, task3] >> task4 >> task5
