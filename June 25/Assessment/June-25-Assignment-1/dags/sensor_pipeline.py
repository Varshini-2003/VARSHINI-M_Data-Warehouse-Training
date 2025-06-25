from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import os
import shutil


FILE_PATH = '/opt/airflow/data/incoming/report.csv'
ARCHIVE_PATH = '/opt/airflow/data/archive/report.csv'


def process_file():
    with open(FILE_PATH, 'r') as f:
        lines = f.readlines()
        print(f"Total lines in report.csv: {len(lines)}")

# Task: Move file to archive
def archive_file():
    os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
    shutil.move(FILE_PATH, ARCHIVE_PATH)
    print(f"Archived to: {ARCHIVE_PATH}")

with DAG(
    dag_id='sensor_pipeline',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'sensor'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_report',
        filepath=FILE_PATH,
        poke_interval=10,     # check every 10 seconds
        timeout=300,          # timeout after 5 minutes
        mode='poke'           # good for SequentialExecutor
    )

    process = PythonOperator(
        task_id='process_report',
        python_callable=process_file
    )

    archive = PythonOperator(
        task_id='archive_report',
        python_callable=archive_file
    )

    wait_for_file >> process >> archive
