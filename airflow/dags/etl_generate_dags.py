from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy as sa
import os
import logging

# Logolás beállítása
logger = logging.getLogger("airflow.task")

# Adatbázis kapcsolat
db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
engine = sa.create_engine(db_url)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dag_structure(dag, pipeline_data):
    """
    Taskok felépítése a pipeline adatok alapján.
    """
    from logic.create import create_table
    from logic.extract import extract_data
    from logic.transform import transform_data
    from logic.load import load_data

    # A paraméterek kinyerése (biztonságosan)
    params = pipeline_data.get('parameters') or {}
    
    # 1. Extract Task - ITT A LÉNYEG: Átadjuk a paramétereket!
    extract_task = PythonOperator(
        task_id=f"extract_data_{pipeline_data['id']}",
        python_callable=extract_data,
        op_kwargs={
            'pipeline_id': pipeline_data['id'],
            'parameters': params,  # <--- Ez a kulcs a WHO indikátorhoz!
            'source_type': pipeline_data.get('source')
        },
        dag=dag,
    )

    # 2. Create Table Task
    create_task = PythonOperator(
        task_id=f"create_table_{pipeline_data['id']}",
        python_callable=create_table,
        op_kwargs={'pipeline_id': pipeline_data['id']},
        dag=dag,
    )

    # 3. Transform Task
    transform_task = PythonOperator(
        task_id=f"transform_data_{pipeline_data['id']}",
        python_callable=transform_data,
        op_kwargs={'pipeline_id': pipeline_data['id']},
        dag=dag,
    )

    # 4. Load Task
    load_task = PythonOperator(
        task_id=f"load_data_{pipeline_data['id']}",
        python_callable=load_data,
        op_kwargs={'pipeline_id': pipeline_data['id']},
        dag=dag,
    )

    # Sorrend: Extract -> Create -> Transform -> Load
    extract_task >> create_task >> transform_task >> load_task

# Pipeline-ok betöltése és DAG generálás
try:
    with engine.connect() as conn:
        # A te adatbázisodban 'etlconfig' a tábla neve
        query = sa.text("SELECT * FROM etlconfig")
        pipelines = conn.execute(query).mappings().all()

    print(f"✅ [ETL Generator] Found {len(pipelines)} pipelines in 'etlconfig'.")

    for pipeline in pipelines:
        try:
            # DAG ID az adatbázisból (pl. pii_v1_d29c7ccb)
            dag_id = pipeline['dag_id']
            if not dag_id:
                safe_name = "".join([c if c.isalnum() else "_" for c in pipeline['pipeline_name']])
                dag_id = f"etl_generated_{pipeline['id']}_{safe_name}"

            # Ütemezés
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
                start_date=datetime(2025, 1, 1),
                catchup=False,
                tags=['JOB', pipeline.get('source', 'unknown')]
            )

            # Konvertáljuk dict-é, hogy módosítható legyen
            create_dag_structure(dag, dict(pipeline))

            # Regisztrálás
            globals()[dag_id] = dag
            print(f"  --> Registered DAG: {dag_id}")

        except Exception as e:
            print(f"❌ Failed to generate DAG for pipeline {pipeline.get('id')}: {e}")

except Exception as e:
    print(f"🔥 CRITICAL ERROR reading etlconfig: {e}")