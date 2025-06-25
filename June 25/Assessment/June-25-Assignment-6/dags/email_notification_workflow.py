from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import random

def process_data():
    print("Processing...")
    if random.choice([True, False]):
        print(" Success!")
    else:
        raise Exception("Simulated failure")

with DAG(
    dag_id='email_notification_workflow',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'email'],
) as dag:

    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    send_success_email = EmailOperator(
        task_id='notify_success',
        to=Variable.get('notification_email'),
        subject=' Task Success Notification',
        html_content='The task <b>process_data</b> completed successfully!',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    send_failure_email = EmailOperator(
        task_id='notify_failure',
        to=Variable.get('notification_email'),
        subject=' Task Failure Notification',
        html_content='The task <b>process_data</b> has failed.',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    process >> [send_success_email, send_failure_email]
