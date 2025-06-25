from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd

file_path = "/opt/airflow/dags/fake_data.csv"

# Step 1: Generate a fake dataset
def generate_dataset():
    df = pd.DataFrame({
        "id": list(range(1, 11)),
        "name": ["user_" + str(i) for i in range(1, 11)],
        "score": [85, 90, 76, 88, 92, 73, 89, 95, 67, 80]
    })
    df.to_csv(file_path, index=False)

# Step 2: Perform a simple quality check
def check_row_count():
    df = pd.read_csv(file_path)
    if len(df) < 50:
        raise ValueError("Row count is too low!")
    print(f"Row count is {len(df)} — OK")

with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["hands-on"]
) as dag:

    generate_data = PythonOperator(
        task_id="generate_dataset",
        python_callable=generate_dataset
    )

    quality_check = PythonOperator(
        task_id="row_count_check",
        python_callable=check_row_count
    )

    log_success = BashOperator(
        task_id="log_quality_passed",
        bash_command='echo "Data Quality Check Passed!"'
    )

    generate_data >> quality_check >> log_success
