from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sync.status_sync import update_pipeline_status

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your.email@example.com'],  # ide mehetnek a hibák
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'status_sync_dag',
    default_args=default_args,
    description='Pipeline status sync from Airflow to database',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 6, 4),    # indulási dátum
    catchup=False,
    max_active_runs=1,
)

status_sync_task = PythonOperator(
    task_id='update_pipeline_status_task',
    python_callable=update_pipeline_status,
    dag=dag,
)

status_sync_task