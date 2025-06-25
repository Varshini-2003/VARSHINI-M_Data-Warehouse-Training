from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,                      # Number of retries
    'retry_delay': timedelta(seconds=5),  # Wait 5s between retries
    'execution_timeout': timedelta(seconds=10),  # Max time allowed per try
}

with DAG(
    dag_id='retry_with_timeout',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'retry', 'timeout'],
) as dag:

    long_task = BashOperator(
        task_id='simulate_timeout',
        bash_command='echo "Starting task..." && sleep 15 && echo "Completed!"'
    )
