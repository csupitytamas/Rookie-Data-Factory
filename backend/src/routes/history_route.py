from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Any
from src.database.connection import get_db
from src.models.status_model import Status
from src.models.etl_config_model import ETLConfig
from src.common.airflow_client import get_dag_run_logs

""" """

router = APIRouter()

@router.get("", response_model=List[Any])
def get_pipeline_history(db: Session = Depends(get_db)):

    #
    try:
        results = (
            db.query(
                Status.etlconfig_id,  
                Status.current_status,
                Status.updated_at,
                ETLConfig.pipeline_name,
                ETLConfig.source
            )
            .join(ETLConfig, Status.etlconfig_id == ETLConfig.id)
            .order_by(Status.updated_at.desc())
            .all()
        )

        #
        history_list = []
        for row in results:
            history_list.append({
                "id": row.etlconfig_id,
                "pipeline_name": row.pipeline_name,
                "source": row.source,
                "status": row.current_status,
                "run_at": row.updated_at,
                "error_message": None
            })
        return history_list
    except Exception as e:
        print(f"Error fetching history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

#
@router.get("/logs/{pipeline_id}")
def get_pipeline_logs(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not pipeline or not pipeline.dag_id:
        raise HTTPException(status_code=404, detail="Pipeline or DAG ID not found")
        
    #
    logs = get_dag_run_logs(pipeline.dag_id, None)
    return {"logs": logs}