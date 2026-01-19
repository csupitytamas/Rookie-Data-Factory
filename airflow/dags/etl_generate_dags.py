from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy as sa
import os
from logic.create import create_table
from logic.extract import extract_data
from logic.transform import transform_data
from logic.load import load_data

# PostgreSQL connection
db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
engine = sa.create_engine(db_url)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with engine.connect() as conn:
    query = sa.text("SELECT * FROM etlconfig")
    pipelines = conn.execute(query).mappings().all()

for pipeline in pipelines:
    dag_id = pipeline['dag_id']
    print(f"[ETL DAG CREATE] dag_id: {dag_id}")
    schedule = pipeline['schedule']
    custom_time = pipeline.get('custom_time')

   # Schedule interval based on pipeline configuration
    if schedule == "daily":
        schedule_interval = "@daily"
    elif schedule == "hourly":
        schedule_interval = "@hourly"
    elif schedule == "weekly":
        schedule_interval = "@weekly"
    elif schedule == "monthly":
        schedule_interval == "@monthly"
    elif schedule == "yearly":
        schedule_interval = "@yearly"
    elif schedule == "once":
        schedule_interval = "@once"
    elif schedule == "never":
        schedule_interval = None
    elif schedule == "custom" and custom_time:
        hour, minute = custom_time.split(':')
        schedule_interval = f"{int(minute)} {int(hour)} * * *"
    else:
        schedule_interval = None


    # DAG definition
    dag = DAG(
        dag_id=dag_id,
        description=f"DAG for {pipeline['pipeline_name']}",
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        is_paused_upon_creation=False
    )
    # Task definitions
    create_task = PythonOperator(
        task_id=f"create_table_{pipeline['id']}",
        python_callable=create_table,
        op_args=[pipeline['id']],
        dag=dag,
    )

    extract_task = PythonOperator(
        task_id=f"extract_data_{pipeline['id']}",
        python_callable=extract_data,
        op_args=[pipeline['id']],
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f"transform_data_{pipeline['id']}",
        python_callable=transform_data,
        op_args=[pipeline['id']],
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f"load_data_{pipeline['id']}",
        python_callable=load_data,
        op_args=[pipeline['id']],
        dag=dag,
    )

    # Task dependencies
    extract_task >>create_task >> transform_task >> load_task

    globals()[dag_id] = dag
