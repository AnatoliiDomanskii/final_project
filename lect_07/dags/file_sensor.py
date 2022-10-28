import datetime as dt

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor


dag = DAG(
    dag_id='file_sensor_example',
    start_date=dt.datetime(2022, 9, 16),
    end_date=dt.datetime(2022, 9, 18),
    schedule_interval="0 5 * * *",
    catchup=True,
)

check_file_exist = FileSensor(
    dag=dag,
    task_id='check_file_exist',
    filepath='sample.txt',
    poke_interval=10,
    timeout=60,
    # 'reschedule' if long timeout and rare intervals
    mode='poke',  # or 'reschedule'
    soft_fail=True,
    email=['admin@example.com']
)

do_some_work = EmptyOperator(
    dag=dag,
    task_id='do_some_work',
)

check_file_exist >> do_some_work
