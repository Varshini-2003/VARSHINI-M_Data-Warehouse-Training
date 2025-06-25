from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
import requests

API_URL = 'https://jsonplaceholder.typicode.com/posts/1'

def fetch_and_validate():
    response = requests.get(API_URL)
    
    if response.status_code != 200:
        raise AirflowFailException(f"API returned status code {response.status_code}")
    
    data = response.json()
    required_fields = ['userId', 'id', 'title', 'body']
    
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise AirflowFailException(f"Missing fields: {missing}")
    
    print("API call succeeded!")
    print(f"Title: {data['title']}")
    print(f"Body: {data['body']}")

with DAG(
    dag_id='external_api_interaction',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assessment', 'api'],
) as dag:

    fetch_api = PythonOperator(
        task_id='call_public_api',
        python_callable=fetch_and_validate
    )
