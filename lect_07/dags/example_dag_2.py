from airflow import DAG
from airflow.models import Variable

from datetime import datetime

from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator


# never use it!
# my_var = Variable.get("my_var")
# print(my_var)


dag = DAG(
    dag_id="example_dag_2",
    start_date=datetime(2022, 9, 16),
    schedule_interval="0 5 * * *",
    catchup=True,
)

copy_file = GCSSynchronizeBucketsOperator(
    dag=dag,
    task_id='copy_file',
    gcp_conn_id='my-gcp-conn',
    source_bucket='my-source_bucket',
    source_object='path/to/{{ ds }}/my_file.csv',
    destination_bucket='my-destination_bucket',
    destination_object='dest/path/{{ ds }}/',
)

send_email_to_boss = EmailOperator(
    task_id='send_email_to_boss',
    dag=dag,
    to=['my_boss@example.com'],
    subject="Increase salary",
    html_content="<b>Give me extra money</b>"
)

copy_file >> send_email_to_boss
