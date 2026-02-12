from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional, Any

# Importáljuk az adatbázis kapcsolatot és a modelleket
from src.database.connection import get_db
from src.models.status_model import Status
from src.models.etl_config_model import ETLConfig
from src.common.airflow_client import get_dag_run_logs

router = APIRouter()

@router.get("/", response_model=List[Any])
def get_pipeline_history(db: Session = Depends(get_db)):
    """
    Visszaadja a pipeline-ok aktuális állapotát (History/Status nézet).
    """
    try:
        # JAVÍTÁS: Status.id helyett Status.etlconfig_id-t használunk
        # JAVÍTÁS: Kivettük a Status.error_message-t, mert nincs a modellben
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

        history_list = []
        for row in results:
            history_list.append({
                "id": row.etlconfig_id,      # A frontendnek átadjuk ID-ként
                "pipeline_name": row.pipeline_name,
                "source": row.source,
                "status": row.current_status,
                "run_at": row.updated_at,
                "error_message": None        # Mivel nincs az DB-ben, üreset adunk vissza
            })
            
        return history_list

    except Exception as e:
        print(f"Error fetching history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/logs/{pipeline_id}")
def get_pipeline_logs(pipeline_id: int, db: Session = Depends(get_db)):
    """
    Visszaadja a pipeline legutóbbi futásának nyers logjait.
    """
    # 1. Megkeressük a pipeline adatait (hogy tudjuk a DAG ID-t)
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    
    if not pipeline or not pipeline.dag_id:
        raise HTTPException(status_code=404, detail="Pipeline or DAG ID not found")
        
    # 2. Lekérjük a logot az Airflow-tól
    logs = get_dag_run_logs(pipeline.dag_id, None) # A dátumot most automatikusan kezeli a fv.
    
    return {"logs": logs}