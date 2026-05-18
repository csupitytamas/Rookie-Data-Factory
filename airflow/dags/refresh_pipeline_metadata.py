from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import sqlalchemy as sa
import os
import json

""" 
    Rendszer DAG, amely a PostgreSQL adatbázisból kiolvassa az pipeline-ok konfigurációját és a rendszer időzónáját, 
    majd ezeket az adatokat JSON formátumban elmenti az Airflow Variables-be. 
    Ezt az adatot használja a DAG generátor (etl_generate_dags.py).
"""

def refresh_metadata():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("Error: Database connection failed.")

    engine = sa.create_engine(db_url)
    with engine.connect() as conn:
        # Aktív pipeline-ok alapvető paramétereinek lekérdezése az etlconfig táblából
        query = sa.text(
        """ SELECT id, pipeline_name, dag_id, schedule, custom_time, source, dependency_pipeline_id FROM etlconfig """)
        pipelines = conn.execute(query).mappings().all()
        pipeline_list = [dict(p) for p in pipelines]
        # Rendszerszintű időzóna beállítás kiolvasása (ha nincs megadva, az alapértelmezett értékkel rendelkezik)
        tz_query = sa.text("SELECT timezone FROM system_settings LIMIT 1")
        tz_result = conn.execute(tz_query).fetchone()
        timezone = tz_result[0] if tz_result and tz_result[0] else "Europe/Budapest"

        # A kinyert adatok összegyűjtése és elmentése az Airflow Variables-be
        metadata = {
            "pipelines": pipeline_list,
            "timezone": timezone,
            "updated_at": datetime.now().isoformat()
        }
        Variable.set("etl_pipeline_metadata", json.dumps(metadata))
        print(f"Metadata updated: {len(pipeline_list)} pipelines saved.")

# DAG definiálása
with DAG(
    dag_id="refresh_pipeline_metadata",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['system', 'metadata']
) as dag:
    PythonOperator(
        task_id="refresh_task",
        python_callable=refresh_metadata
    )
