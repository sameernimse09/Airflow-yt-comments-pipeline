from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from yt_etl import run_yt_comments  # Ensure yt_etl.py is in the same folder


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start a day before today
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'yt_etl_dag',
    default_args=default_args,
    description='DAG with ETL process!',
    schedule_interval='@daily',  # Run daily
)

# Description of the DAG (optional)
dag.doc_md = """
### YouTube ETL DAG
This DAG runs an ETL process that extracts YouTube data, processes comments, and loads the processed data.
"""

# Define the task
run_etl = PythonOperator(
    task_id='complete_yt_etl',
    python_callable=run_yt_comments,  
    dag=dag,
)

run_etl
