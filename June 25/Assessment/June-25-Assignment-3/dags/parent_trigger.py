from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='parent_trigger',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'trigger'],
) as dag:

    trigger = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='child_target',
        conf={
            'source': 'parent_trigger',
            'note': 'Triggered from parent DAG'
        },
        wait_for_completion=False
    )
