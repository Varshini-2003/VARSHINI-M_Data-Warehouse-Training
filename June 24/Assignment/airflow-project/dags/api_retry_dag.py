from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random
import logging

def flaky_api():
    """Simulates a flaky API call with 50% chance of failure."""
    if random.random() < 0.5:
        raise Exception("Simulated API failure")
    print("API call succeeded!")

def log_alert():
    logging.error("API call failed after all retries!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,  # Total 3 retries
    'retry_delay': timedelta(seconds=10),
    'retry_exponential_backoff': True,  
    'max_retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG(
    'flaky_api_with_retry_alert',
    default_args=default_args,
    schedule_interval=None,
    description="Simulate flaky API with retries and alert",
    tags=['retry', 'alert'],
) as dag:

    try_api_call = PythonOperator(
        task_id='try_flaky_api',
        python_callable=flaky_api
    )

    log_failure = PythonOperator(
        task_id='log_final_failure',
        python_callable=log_alert,
        trigger_rule='one_failed' 
    )

    success_follow_up = EmptyOperator(
        task_id='follow_up_if_successful',
        trigger_rule='all_success'  
    )

    try_api_call >> [log_failure, success_follow_up]
