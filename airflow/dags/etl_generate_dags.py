from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy as sa
import os
import logging
import pendulum


logger = logging.getLogger("airflow.task")
db_url = os.getenv("DB_URL")
if not db_url:
    raise ValueError("Error: Database connection failed.")
engine = sa.create_engine(db_url)
def get_user_timezone():
    try:
        with engine.connect() as conn:
            query = sa.text("SELECT timezone FROM system_settings LIMIT 1")
            result = conn.execute(query).fetchone()
            if result and result[0]:
                return result[0]
    except Exception as e:
        print(f"Fetch error {e}")
    return "Europe/Budapest"

user_tz_name = get_user_timezone()
local_tz = pendulum.timezone(user_tz_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dag_structure(dag, pipeline_data):

    from logic.create import create_table
    from logic.extract import extract_data
    from logic.transform import transform_data
    from logic.load import load_data

    params = pipeline_data.get('parameters') or {}
    
    extract_task = PythonOperator(
        task_id=f"extract_data_{pipeline_data['id']}",
        python_callable=extract_data,
        op_kwargs={
            'pipeline_id': pipeline_data['id'],
            'parameters': params,
            'source_type': pipeline_data.get('source'),
            'dependency_id': pipeline_data.get('dependency_pipeline_id') 
        },
        dag=dag,
    )

    create_task = PythonOperator(
        task_id=f"create_table_{pipeline_data['id']}",
        python_callable=create_table,
        op_kwargs={'pipeline_id': pipeline_data['id']},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f"transform_data_{pipeline_data['id']}",
        python_callable=transform_data,
        op_kwargs={'pipeline_id': pipeline_data['id']},
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f"load_data_{pipeline_data['id']}",
        python_callable=load_data,
        op_kwargs={'pipeline_id': pipeline_data['id']},
        dag=dag,
    )

    extract_task >> create_task >> transform_task >> load_task

try:
    with engine.connect() as conn:
        query = sa.text("SELECT * FROM etlconfig")
        pipelines = conn.execute(query).mappings().all()

    for pipeline in pipelines:
        try:
            dag_id = pipeline['dag_id']
            if not dag_id:
                safe_name = "".join([c if c.isalnum() else "_" for c in pipeline['pipeline_name']])
                dag_id = f"etl_generated_{pipeline['id']}_{safe_name}"

            schedule = pipeline.get('schedule', 'daily')
            if schedule == 'custom' and pipeline.get('custom_time'):
                hour, minute = pipeline['custom_time'].split(':')
                schedule_interval = f"{int(minute)} {int(hour)} * * *"
            elif schedule in ['daily', 'hourly', 'weekly', 'monthly']:
                schedule_interval = f"@{schedule}"
            else:
                schedule_interval = None 

            dag = DAG(
                dag_id=dag_id,
                description=f"JOB for {pipeline['pipeline_name']}",
                default_args=default_args,
                schedule_interval=schedule_interval,
                start_date=datetime(2025, 1, 1, tzinfo=local_tz),
                catchup=False,
                tags=['JOB', pipeline.get('source', 'unknown')]
            )

            create_dag_structure(dag, dict(pipeline))
            globals()[dag_id] = dag

        except Exception as e:
            print(f"Failed to generate {pipeline.get('id')}: {e}")

except Exception as e:
    print(f" Error: {e}")