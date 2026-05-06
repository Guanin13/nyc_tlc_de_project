from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from src.extract import extract

# Define the DAG (workflow)
with DAG(
    'nyc_taxi_pipeline',
    description='A DAG to extract data from the NYC TLC website and upload it to S3.' \
    'After that, the data will be transformed using dbt models to silver layer',
    schedule='@monthly',  # Run once every month
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,  # First logical date for the DAG
    catchup=True  # Run past scheduled periods that were missed
) as dag:

    # Define a task that calls the extract() function
    extract_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract, 
        retries = 3,
        retry_delay=timedelta(minutes=2),  # Wait 2 minutes before retrying
        op_kwargs={
            'ds': '{{ ds }}'
        },
    )

    run_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": "nyc-taxi-raw-crawler"},
        aws_conn_id="aws_default",
        wait_for_completion=False,
    )

    wait_for_crawler = GlueCrawlerSensor(
        task_id="wait_for_glue_crawler",
        crawler_name="nyc-taxi-raw-crawler",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 30,
    )

    dbt_run_silver = BashOperator(
        task_id = 'dbt_run',
        bash_command = 'cd /opt/airflow/dbt_project && dbt run --select silver_yellow_trip',
    )

    dbt_test_silver = BashOperator(
        task_id = 'dbt_test',
        bash_command = 'cd /opt/airflow/dbt_project && dbt test --select silver_yellow_trip',
    )

    # Define task dependencies
    extract_task >> run_crawler >> wait_for_crawler >> dbt_run_silver >> dbt_test_silver