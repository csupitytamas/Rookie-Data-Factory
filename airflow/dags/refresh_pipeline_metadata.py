from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import sqlalchemy as sa
import os
import json

def refresh_metadata():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("Error: Database connection failed.")
    
    engine = sa.create_engine(db_url)
    
    with engine.connect() as conn:
        # Csak a DAG generáláshoz SZÜKSÉGES mezőket kérjük le
        query = sa.text("""
            SELECT id, pipeline_name, dag_id, schedule, custom_time, source, dependency_pipeline_id 
            FROM etlconfig
        """)
        pipelines = conn.execute(query).mappings().all()
        
        # Konvertáljuk listává, amit JSON-ba lehet tenni
        pipeline_list = [dict(p) for p in pipelines]
        
        # Lekérjük a timezone-t is, hogy ne kelljen a generátornak a DB-hez nyúlnia
        tz_query = sa.text("SELECT timezone FROM system_settings LIMIT 1")
        tz_result = conn.execute(tz_query).fetchone()
        timezone = tz_result[0] if tz_result and tz_result[0] else "Europe/Budapest"
        
        # Elmentjük az Airflow Variable-be
        metadata = {
            "pipelines": pipeline_list,
            "timezone": timezone,
            "updated_at": datetime.utcnow().isoformat()
        }
        Variable.set("etl_pipeline_metadata", json.dumps(metadata))
        print(f"🚀 Metadata frissítve: {len(pipeline_list)} pipeline elmentve.")

with DAG(
    dag_id="refresh_pipeline_metadata",
    schedule_interval="@hourly", # Biztonsági tartalék, ha a backend nem hívná meg
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['system', 'metadata']
) as dag:
    PythonOperator(
        task_id="refresh_task",
        python_callable=refresh_metadata
    )
