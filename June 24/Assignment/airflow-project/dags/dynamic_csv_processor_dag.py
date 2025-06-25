from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import os
import pandas as pd

INPUT_DIR = '/opt/airflow/data/input'
OUTPUT_DIR = '/opt/airflow/data/output'
EXPECTED_HEADERS = ['id', 'name', 'value']

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['dynamic', 'csv', 'taskflow']
)
def dynamic_csv_processing_dag():

    @task()
    def list_csv_files():
        files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
        return files

    @task()
    def process_file(filename: str):
        file_path = os.path.join(INPUT_DIR, filename)
        df = pd.read_csv(file_path)

        # Step 1: Validate headers
        actual_headers = df.columns.tolist()
        if actual_headers != EXPECTED_HEADERS:
            raise ValueError(f"Invalid headers in {filename}. Found {actual_headers}")

        # Step 2: Calculate row count
        row_count = len(df)

        # Step 3: Save individual summary
        summary = pd.DataFrame([{
            'filename': filename,
            'row_count': row_count
        }])
        summary_path = os.path.join(OUTPUT_DIR, f'summary_{filename}')
        summary.to_csv(summary_path, index=False)
        return summary_path

    @task()
    def merge_summaries(summary_paths: list):
        merged_df = pd.concat([pd.read_csv(path) for path in summary_paths], ignore_index=True)
        final_path = os.path.join(OUTPUT_DIR, 'summary.csv')
        merged_df.to_csv(final_path, index=False)
        print(f"Final summary saved to {final_path}")

    file_list = list_csv_files()
    summary_paths = process_file.expand(filename=file_list)
    merge_summaries(summary_paths)

dynamic_csv_processing_dag = dynamic_csv_processing_dag()
