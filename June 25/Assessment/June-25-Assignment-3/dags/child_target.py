from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def log_metadata(**kwargs):
    run_id = kwargs['run_id']
    execution_date = kwargs['execution_date']
    conf = kwargs.get('dag_run').conf
    print(f"Child DAG triggered by: {conf.get('source')}")
    print(f"Run ID: {run_id}")
    print(f"Execution Date: {execution_date}")
    print(f"Note: {conf.get('note')}")

with DAG(
    dag_id='child_target',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'triggered'],
) as dag:

    log = PythonOperator(
        task_id='log_trigger_metadata',
        python_callable=log_metadata,
        provide_context=True
    )
