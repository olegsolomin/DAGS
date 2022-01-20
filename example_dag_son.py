# Python libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021,12,9),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# DAG definition
dag=DAG('example_dag_son',
    description='A simple DAG made by SON',
    default_args=default_args,
    schedule_interval=dt.timedelta(seconds=5),
)

# TASK definition
task1 = BashOperator(
    task_id='print_hello',
    bash_command='echo \'Greetings. The date and time are \'',
    dag=dag,
)

task2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# TASK pipeline

task1 >> task2
