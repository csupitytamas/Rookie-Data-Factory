from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import logging
import pendulum
import json

logger = logging.getLogger("airflow.task")

metadata_raw = Variable.get("etl_pipeline_metadata", default_var=None)
if metadata_raw:
    metadata = json.loads(metadata_raw)
    pipelines = metadata.get("pipelines", [])
    user_tz_name = metadata.get("timezone", "Europe/Budapest")
else:
    pipelines = []
    user_tz_name = "Europe/Budapest"

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

    # A paramétereket és egyéb adatokat a Task-on belül fogják lekérni a DB-ből a pipeline_id alapján
    extract_task = PythonOperator(
        task_id=f"extract_data_{pipeline_data['id']}",
        python_callable=extract_data,
        op_kwargs={
            'pipeline_id': pipeline_data['id'],
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

# DAG-ok generálása a Variable-ből jövő lista alapján
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
        elif schedule in ['daily', 'hourly', 'weekly', 'monthly', '@daily', '@hourly', '@weekly', '@monthly']:
            schedule_interval = schedule if schedule.startswith('@') else f"@{schedule}"
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

        create_dag_structure(dag, pipeline)
        globals()[dag_id] = dag

    except Exception as e:
        logger.error(f"Failed to generate DAG for pipeline {pipeline.get('id')}: {e}")
