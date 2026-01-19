from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session, joinedload
from src.models.status_model import Status
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.schemas import DashboardPipelineResponse
from src.database.connection import get_db
import pandas as pd

router = APIRouter()

@router.get("/dashboard", response_model=list[DashboardPipelineResponse])
def get_dashboard(
    db: Session = Depends(get_db),
):
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
        sample_data = []
        if pipeline.target_table_name:
            try:
                sql = f"SELECT * FROM {pipeline.target_table_name}"
                df = pd.read_sql(sql, db.bind)
                sample_data = df.to_dict(orient="records")
            except Exception:
                sample_data = []

        status = pipeline.status[0] if pipeline.status else None

        result.append({
            "id": pipeline.id,
            "name": pipeline.pipeline_name,
            "lastRun": status.last_successful_run.strftime("%Y-%m-%d %H:%M") if status and status.last_successful_run else None,
            "status": status.current_status if status else None,
            "nextRun": status.next_scheduled_run.strftime("%Y-%m-%d %H:%M") if status and status.next_scheduled_run else None,
            "source": pipeline.source,
            "alias": pipeline.schema.alias if pipeline.schema else None,
            "sampleData": sample_data
        })
    return result