from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import pandas as pd

CSV_PATH = '/opt/airflow/data/customers.csv'  

def check_file_exists():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"{CSV_PATH} not found.")

def count_rows(**kwargs):
    df = pd.read_csv(CSV_PATH)
    row_count = len(df)
    kwargs['ti'].xcom_push(key='row_count', value=row_count)
    print(f"Total rows: {row_count}")

def check_row_threshold(**kwargs):
    row_count = kwargs['ti'].xcom_pull(key='row_count', task_ids='count_rows')
    if row_count > 100:
        return 'send_message'
    else:
        return 'no_action'

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG('csv_to_summary_dag',
         default_args=default_args,
         schedule_interval=None,
         description='Read a CSV and log row count, send message if count > 100',
         tags=['csv', 'summary'],
         ) as dag:

    check_file = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists
    )

    count = PythonOperator(
        task_id='count_rows',
        python_callable=count_rows,
        provide_context=True
    )

    check_threshold = BranchPythonOperator(
        task_id='check_threshold',
        python_callable=check_row_threshold,
        provide_context=True
    )

    send_message = BashOperator(
        task_id='send_message',
        bash_command='echo "Row count is greater than 100!"'
    )

    no_action = EmptyOperator(task_id='no_action')

    end = EmptyOperator(task_id='end')

    check_file >> count >> check_threshold
    check_threshold >> [send_message, no_action]
    send_message >> end
    no_action >> end
