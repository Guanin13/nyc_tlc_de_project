from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from src.extract import extract

# Define the DAG (workflow)
with DAG(
    'extract_dag',
    description='A DAG to extract data from the NYC TLC website and upload it to S3',
    schedule='@monthly',  # Run once every month
    start_date=datetime(2025, 1, 1),  # First logical date for the DAG
    catchup=True  # Run past scheduled periods that were missed
) as dag:

    # Define a task that calls the extract() function
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract, 
        retries = 3,
        retry_delay=timedelta(minutes=2),  # Wait 2 minutes before retrying
        op_kwargs={
            'ds': '{{ ds }}'
        },
    )