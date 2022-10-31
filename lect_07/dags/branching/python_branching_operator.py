import datetime as dt

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance
from airflow.models.baseoperator import chain


dag = DAG(
    dag_id="python_branching_operator",
    start_date=dt.datetime(2022, 10, 25),
    end_date=dt.datetime(2022, 10, 28),
    schedule_interval="@daily",
    catchup=True,
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)


def select_branch(ti: TaskInstance):
    if ti.execution_date.day == 25:
        return ['task1', 'task2']
    else:
        return ['task3', "task4"]


branching = BranchPythonOperator(
    task_id='branching',
    dag=dag,
    python_callable=select_branch,
)


task1 = EmptyOperator(
    task_id='task1',
    dag=dag,
)

task2 = EmptyOperator(
    task_id='task2',
    dag=dag,
)

task3 = EmptyOperator(
    task_id='task3',
    dag=dag,
)

task4 = EmptyOperator(
    task_id='task4',
    dag=dag,
)


end = EmptyOperator(
    task_id='end',
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)


# start.set_downstream(branching)
#
# branching.set_downstream(task1)
# branching.set_downstream(task3)
#
# task1.set_downstream(task2)
# task3.set_downstream(task4)
#
# end.set_upstream([task2, task4])

chain(start, branching, [task1, task3], [task2, task4], end)
