from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session, joinedload
from src.models.status_model import Status
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.schemas import DashboardPipelineResponse
from src.database.connection import get_db
import pandas as pd
from datetime import timedelta

router = APIRouter()

@router.get("/dashboard", response_model=list[DashboardPipelineResponse])
def get_dashboard(db: Session = Depends(get_db)):
    # Dashboard preview: marad a 11 soros limit a gyorsaságért
    pipelines = (
        db.query(ETLConfig)
        .outerjoin(Status, ETLConfig.id == Status.etlconfig_id)
        .outerjoin(APISchema, ETLConfig.source == APISchema.source)
        .options(joinedload(ETLConfig.schema))
        .options(joinedload(ETLConfig.status))
        .all()
    )
    result = []
    for pipeline in pipelines:
        # Meghatározzuk a hidden oszlopokat a field_mappings-ből
        hidden_columns = []
        if pipeline.field_mappings:
            for orig_col, props in pipeline.field_mappings.items():
                if props.get("hidden"):
                    # Ha át van nevezve, akkor az új nevet kell elrejteni a dashboardon
                    final_name = props.get("newName") if props.get("rename") and props.get("newName") else orig_col
                    hidden_columns.append(final_name)

        sample_data = []
        if pipeline.target_table_name:
            try:
                # Preview: LIMIT 11
                sql = f'SELECT * FROM "{pipeline.target_table_name}" LIMIT 11'
                df = pd.read_sql(sql, db.bind)
                
                # Hidden oszlopok kiszűrése
                cols_to_drop = [c for c in hidden_columns if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)

                df = df.astype(object).where(pd.notnull(df), None)
                sample_data = df.to_dict(orient="records")
            except Exception:
                sample_data = []

        status = pipeline.status[0] if pipeline.status else None

        # Airflow-specifikus korrekció: A megjelenített "Next update" a jövőbe mutasson
        next_run_display = None
        if status and status.next_scheduled_run:
            try:
                actual_next_run = status.next_scheduled_run
                sched = str(pipeline.schedule).strip().lower()
                
                # Kiszámoljuk, mikor fog TÉNYLEGESEN lefutni (intervallum vége)
                if sched in ['daily', '@daily', 'custom']:
                    actual_next_run += timedelta(days=1)
                elif sched in ['hourly', '@hourly']:
                    actual_next_run += timedelta(hours=1)
                elif sched in ['weekly', '@weekly']:
                    actual_next_run += timedelta(weeks=1)
                elif sched in ['monthly', '@monthly']:
                    actual_next_run += timedelta(days=30)
                    
                next_run_display = actual_next_run.strftime("%Y-%m-%d %H:%M")
            except Exception:
                next_run_display = None

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
            "alias": pipeline.schema.alias if pipeline.schema else None,
            "sampleData": sample_data
        })
    return result

# 🟢 TISZTA ROUTE: Csak a teljes tábla lekérdezése
@router.get("/dashboard/pipeline/{pipeline_id}/data")
def get_pipeline_full_data(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()

    if not pipeline or not pipeline.target_table_name:
        return {"data": []}

    try:
        # NINCS LIMIT, NINCS FELTÉTEL: SELECT * FROM ...
        query = f'SELECT * FROM "{pipeline.target_table_name}"'
        df = pd.read_sql(query, db.bind)

        # Hidden oszlopok kiszűrése
        hidden_columns = []
        if pipeline.field_mappings:
            for orig_col, props in pipeline.field_mappings.items():
                if props.get("hidden"):
                    final_name = props.get("newName") if props.get("rename") and props.get("newName") else orig_col
                    hidden_columns.append(final_name)
        
        cols_to_drop = [c for c in hidden_columns if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # JSON kompatibilitás miatt a NaN-okat kiszedjük
        df = df.astype(object).where(pd.notnull(df), None)
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        print(f"Error: {e}")
        return {"data": []}