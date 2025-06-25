from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
import pandas as pd
import os

ORDERS_PATH = '/opt/airflow/data/orders.csv'
REQUIRED_COLUMNS = ['order_id', 'customer_id', 'amount']

def validate_orders():
    if not os.path.exists(ORDERS_PATH):
        raise AirflowFailException("orders.csv not found")

    df = pd.read_csv(ORDERS_PATH)
    
    # Check columns exist
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise AirflowFailException(f"Missing required columns: {missing_columns}")
    
    # Check for nulls
    nulls = df[REQUIRED_COLUMNS].isnull().sum()
    null_report = nulls[nulls > 0]
    if not null_report.empty:
        raise AirflowFailException(f"Null values found:\n{null_report}")
    
    print("Data quality check passed.")

with DAG(
    dag_id='validate_orders',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'validation'],
) as dag:

    validate = PythonOperator(
        task_id='validate_orders_csv',
        python_callable=validate_orders
    )
