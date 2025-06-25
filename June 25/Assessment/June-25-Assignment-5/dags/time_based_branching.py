from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def choose_branch():
    now = datetime.now().hour
    print(f"Current hour: {now}")
    if 6 <= now < 12:
        return 'morning_task'
    elif 18 <= now < 24:
        return 'evening_task'
    else:
        return 'off_hours_task'

with DAG(
    dag_id='time_based_branching',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'branching', 'time'],
) as dag:

    branch = BranchPythonOperator(
        task_id='choose_time_branch',
        python_callable=choose_branch
    )

    morning = BashOperator(
        task_id='morning_task',
        bash_command='echo "Good morning! Running morning task."'
    )

    evening = BashOperator(
        task_id='evening_task',
        bash_command='echo "Good evening! Running evening task."'
    )

    off_hours = BashOperator(
        task_id='off_hours_task',
        bash_command='echo "Running during off-hours."'
    )

    branch >> [morning, evening, off_hours]
