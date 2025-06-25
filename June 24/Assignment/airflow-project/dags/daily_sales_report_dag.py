from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil

# Define paths
SALES_PATH = '/opt/airflow/data/sales_data.csv'
SUMMARY_PATH = '/opt/airflow/data/sales_summary.csv'
ARCHIVE_DIR = '/opt/airflow/archive/'

def read_and_summarize_sales():
    if not os.path.exists(SALES_PATH):
        raise FileNotFoundError(f"{SALES_PATH} not found.")

    df = pd.read_csv(SALES_PATH)
    summary = df.groupby('category')['amount'].sum().reset_index()
    summary.to_csv(SUMMARY_PATH, index=False)
    print("Summary saved:", summary)

def archive_original_file():
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archived_file = os.path.join(ARCHIVE_DIR, f'sales_{timestamp}.csv')
    shutil.move(SALES_PATH, archived_file)
    print(f"Archived to {archived_file}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=5),  # ⏰ Bonus: Max 5 min per task
    'catchup': False,
}

with DAG('daily_sales_report_dag',
         default_args=default_args,
         schedule_interval='0 6 * * *',  # Every day at 6 AM
         description='Summarize sales daily and archive original',
         tags=['sales', 'daily', 'report']
         ) as dag:

    summarize_task = PythonOperator(
        task_id='summarize_sales',
        python_callable=read_and_summarize_sales
    )

    archive_task = PythonOperator(
        task_id='archive_file',
        python_callable=archive_original_file
    )

    summarize_task >> archive_task
