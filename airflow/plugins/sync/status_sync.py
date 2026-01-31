from sqlalchemy import create_engine, select, update, insert, Table, MetaData, Column, Integer, String, TIMESTAMP, Interval
from sqlalchemy.orm import sessionmaker
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime
from sync.api_client import get_next_scheduled_run
from sqlalchemy.dialects.postgresql import insert as pg_insert
import re

AIRFLOW_DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
airflow_engine = create_engine(AIRFLOW_DB_URL)
AirflowSession = sessionmaker(bind=airflow_engine)



ETL_DB_URL = "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL"
etl_engine = create_engine(ETL_DB_URL)
ETLSession = sessionmaker(bind=etl_engine)
metadata = MetaData()

# ETL tábla definíciók (status, status_history)
etlconfig = Table(
    'etlconfig', metadata,
    Column('id', Integer, primary_key=True),
    Column('pipeline_name', String),
    Column('source', String),
    Column('schedule', String),
    Column('custom_time', String),
    Column('target_table_name', String),
    Column('dag_id', String),
)

status = Table(
    'status', metadata,
    Column('etlconfig_id', Integer, primary_key=True),
    Column('current_status', String),
    Column('last_successful_run', TIMESTAMP),
    Column('next_scheduled_run', TIMESTAMP),
    Column('execution_time', Interval),
    Column('updated_at', TIMESTAMP),
)

status_history = Table(
    'status_history', metadata,
    Column('id', Integer, primary_key=True),
    Column('etlconfig_id', Integer),
    Column('status', String),
    Column('changed_at', TIMESTAMP),
    Column('execution_time', Interval),
    Column('message', String),
)
def update_pipeline_status():
    with ETLSession() as etl_session:
        pipelines = etl_session.execute(select(
            etlconfig.c.id,
            etlconfig.c.pipeline_name,
            etlconfig.c.dag_id
        )).fetchall()

        with AirflowSession() as airflow_session:
            dag_ids = airflow_session.scalars(select(DagRun.dag_id.distinct())).all()
            print(f"[DEBUG] Összes DAG ID az Airflow-ban ({len(dag_ids)} db): {dag_ids}")

            for pipeline in pipelines:
                etlconfig_id = pipeline.id
                pipeline_name = pipeline.pipeline_name.lower().replace(' ', '_')
                dag_id = pipeline.dag_id
                print(f"\n[DEBUG] Pipeline ID: {etlconfig_id}, pipeline_name (sanitized): {pipeline_name}, dag_id: {dag_id}")

                # ÚJ: Ellenőrizzük az aktuális status-t
                current_status_query = select(status.c.current_status).where(status.c.etlconfig_id == etlconfig_id)
                current_status_value = etl_session.execute(current_status_query).scalar()

                if current_status_value == "archived":
                    print(f"[INFO] Pipeline {etlconfig_id} archivált státuszban van, kihagyva a frissítést.")
                    continue

                if dag_id not in dag_ids:
                    print(f"[WARNING] Nem található az Airflow-ban ez a DAG ID: {dag_id}")
                    continue

                dag_runs = airflow_session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).all()
                if not dag_runs:
                    print(f"[WARNING] Nincsenek futtatások a DAG-hoz: {dag_id}")
                    continue

                latest_run = dag_runs[0]
                current_status = latest_run.state
                last_successful_run = None

                for run in dag_runs:
                    if run.state == State.SUCCESS:
                        last_successful_run = run.execution_date
                        break

                execution_time = None
                if latest_run.start_date and latest_run.end_date:
                    execution_time = latest_run.end_date - latest_run.start_date

                next_run = get_next_scheduled_run(dag_id)

                # Update status tábla
                stmt = update(status).where(
                    status.c.etlconfig_id == etlconfig_id
                ).values(
                    current_status=current_status,
                    last_successful_run=last_successful_run,
                    next_scheduled_run=next_run,
                    execution_time=execution_time,
                    updated_at=datetime.utcnow()
                )

                etl_session.execute(stmt)

                # Insert status_history tábla
                history_stmt = insert(status_history).values(
                    etlconfig_id=etlconfig_id,
                    status=current_status,
                    execution_time=execution_time,
                    changed_at=datetime.utcnow(),
                    message=str(latest_run.external_trigger)
                )

                upsert_stmt = pg_insert(status).values(
                    etlconfig_id=etlconfig_id,
                    current_status=current_status,
                    last_successful_run=last_successful_run,
                    next_scheduled_run=next_run,
                    execution_time=execution_time,
                    updated_at=datetime.utcnow()
                ).on_conflict_do_update(
                    index_elements=[status.c.etlconfig_id],
                    set_={
                        "current_status": current_status,
                        "last_successful_run": last_successful_run,
                        "next_scheduled_run": next_run,
                        "execution_time": execution_time,
                        "updated_at": datetime.utcnow()
                    }
                )
                etl_session.execute(upsert_stmt)

                # Insert status_history tábla (ez marad, hisz mindig új sor kell)
                history_stmt = insert(status_history).values(
                    etlconfig_id=etlconfig_id,
                    status=current_status,
                    execution_time=execution_time,
                    changed_at=datetime.utcnow(),
                    message=str(latest_run.external_trigger)
                )

                etl_session.execute(history_stmt)

            etl_session.commit()