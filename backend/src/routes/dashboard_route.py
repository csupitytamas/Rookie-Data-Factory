from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session, joinedload
from src.models.status_model import Status
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.schemas import DashboardPipelineResponse
from src.database.connection import get_db
from src.common.airflow_client import get_dag_runs_history, get_airflow_health
import pandas as pd
from datetime import timedelta

"""
A modul a dashboard-hoz szükséges HTTP(CRUD) végpontokat tartalmazza, beleértve a pipeline-ok állapotát, futtatási előzményeit és adatainak előnézetét.
"""

router = APIRouter()


# Airflow health check
@router.get("/health")
def airflow_health():
    return get_airflow_health()


# Lekéri egy adott pipeline Airflow futtatási előzményeit (utolsó 12 futás).
@router.get("/dashboard/pipeline/{pipeline_id}/history")
def get_pipeline_airflow_history(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not pipeline or not pipeline.dag_id:
        return []
    return get_dag_runs_history(pipeline.dag_id, limit=12)


# Összeállítja a dashboardon szükséges adatokat az összes pipeline-hoz
@router.get("/dashboard", response_model=list[DashboardPipelineResponse])
def get_dashboard(db: Session = Depends(get_db)):
    pipelines = (
        db.query(ETLConfig)
        .outerjoin(Status, ETLConfig.id == Status.etlconfig_id)
        .outerjoin(APISchema, ETLConfig.source == APISchema.source)
        .options(joinedload(ETLConfig.schema))
        .options(joinedload(ETLConfig.status))
        .all()
    )
    result = []

    # Végigmegyünk a pipeline-okon és kigyűjtjük a megjelenítendő információkat, figyelembe véve a rejtett oszlopokat.
    for pipeline in pipelines:
        hidden_columns = []
        if pipeline.field_mappings:
            for orig_col, props in pipeline.field_mappings.items():
                if props.get("hidden"):
                    final_name = props.get("newName") if props.get("rename") and props.get("newName") else orig_col
                    hidden_columns.append(final_name)

        sample_data = []

        # Ha a pipeline rendelkezik céltáblával, lekérünk egy mintát az adatokból.
        if pipeline.target_table_name:

            # Mintadatok lekérése (első 11 sor) a céltáblából a dashboard előnézethez.
            try:
                sql = f'SELECT * FROM "{pipeline.target_table_name}" LIMIT 11'
                df = pd.read_sql(sql, db.bind)
                cols_to_drop = [c for c in hidden_columns if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)

                # NaN értékek és üres stringek kezelése.
                df = df.astype(object).where(pd.notna(df), None)
                df = df.replace(r'^\s*$', None, regex=True)
                sample_data = df.to_dict(orient="records")
            except Exception:
                sample_data = []
        status = pipeline.status[0] if pipeline.status else None
        next_run_display = None

        # A következő esedékes futás időpontjának becslése a pipeline ütemezése alapján.
        if status and status.next_scheduled_run:
            try:
                actual_next_run = status.next_scheduled_run
                sched = str(pipeline.schedule).strip().lower()
                if sched in ['daily', '@daily', 'custom']:
                    actual_next_run += timedelta(days=1)
                elif sched in ['hourly', '@hourly']:
                    actual_next_run += timedelta(hours=1)
                elif sched in ['weekly', '@weekly']:
                    actual_next_run += timedelta(weeks=1)
                elif sched in ['monthly', '@monthly']:
                    actual_next_run += timedelta(days=30)

                # Időpont formázása a felhasználói felület számára.
                next_run_display = actual_next_run.strftime("%Y-%m-%d %H:%M")
            except Exception:
                next_run_display = None

        # Az utolsó sikeres futás időpontjának formázása.
        last_run_display = None
        if status and status.last_successful_run:
            try:
                last_run_display = status.last_successful_run.strftime("%Y-%m-%d %H:%M")
            except Exception:
                last_run_display = None
        result.append({
            "id": pipeline.id,
            "name": pipeline.pipeline_name,
            "lastRun": last_run_display,
            "status": status.current_status if status else None,
            "nextRun": next_run_display,
            "source": pipeline.source,
            "dag_id": pipeline.dag_id,
            "alias": pipeline.schema.alias if pipeline.schema else None,
            "save_option": pipeline.save_option,
            "sampleData": sample_data
        })
    return result

# Endpoint a PowerBI export számára.
@router.get("/dashboard/pipeline/{pipeline_id}/powerbi")
def get_pipeline_powerbi_data(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not pipeline or not pipeline.target_table_name:
        return []

    # A teljes adattábla lekérése és formázása az exportáláshoz.
    try:
        query = f'SELECT * FROM "{pipeline.target_table_name}"'
        df = pd.read_sql(query, db.bind)
        hidden_columns = []
        if pipeline.field_mappings:
            for orig_col, props in pipeline.field_mappings.items():
                if props.get("hidden"):
                    final_name = props.get("newName") if props.get("rename") and props.get("newName") else orig_col
                    hidden_columns.append(final_name)

        # A rejtett oszlopok eltávolítása az exportált adatkészletből.
        cols_to_drop = [c for c in hidden_columns if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # Adatok tisztítása és formázása.
        df = df.astype(object).where(pd.notna(df), None)
        df = df.replace(r'^\s*$', None, regex=True)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"PowerBI Export Error: {e}")
        return []

# Lekéri egy adott pipeline teljes adattartalmát a céltáblából.
@router.get("/dashboard/pipeline/{pipeline_id}/data")
def get_pipeline_full_data(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not pipeline or not pipeline.target_table_name:
        return {"data": []}

    # Az összes sor lekérdezése és a rejtett oszlopok szűrése.
    try:
        query = f'SELECT * FROM "{pipeline.target_table_name}"'
        df = pd.read_sql(query, db.bind)
        hidden_columns = []
        if pipeline.field_mappings:
            for orig_col, props in pipeline.field_mappings.items():
                if props.get("hidden"):
                    final_name = props.get("newName") if props.get("rename") and props.get("newName") else orig_col
                    hidden_columns.append(final_name)

        # Rejtett oszlopok eltávolítása a válaszból.
        cols_to_drop = [c for c in hidden_columns if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        df = df.astype(object).where(pd.notna(df), None)
        df = df.replace(r'^\s*$', None, regex=True)
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        print(f"Error: {e}")
        return {"data": []}