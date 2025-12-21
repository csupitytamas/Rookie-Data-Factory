
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from src.schemas import  ETLConfigBase, ETLConfigUpdate, ETLConfigResponse
from src.database.connection import get_db
from src.models.etl_config_model import ETLConfig
from src.models.status_model import Status
from src.models.api_schemas_model import APISchema
from src.utils.db_creation_util import generate_table_name, generate_dag_id, remove_version_suffix
from src.constans.accepted_fields import ACCEPTED_ETL_FIELDS
from src.common.airflow_client import pause_airflow_dag, unpause_airflow_dag
from datetime import datetime


router = APIRouter()


@router.post("/create", response_model=ETLConfigResponse)
def create_pipeline(
    config: ETLConfigBase,
    db: Session = Depends(get_db),
):
    try:
        config_dict = config.dict()
        filtered_data = {k: v for k, v in config_dict.items() if k in ACCEPTED_ETL_FIELDS}

        schema = db.query(APISchema).filter_by(source=config.source).first()
        if not schema:
            raise HTTPException(status_code=404, detail="No schema found for the selected source.")

        table_name = generate_table_name(config.pipeline_name, version=1)
        dag_id = generate_dag_id(config.pipeline_name, version=1)

        filtered_data["target_table_name"] = table_name
        filtered_data["dag_id"] = dag_id

        new_pipeline = ETLConfig(**filtered_data, version=1)

        db.add(new_pipeline)
        db.commit()
        db.refresh(new_pipeline)

        try:
            unpause_airflow_dag(new_pipeline.dag_id)
        except Exception as e:
            print(f"⚠️ Nem sikerült unpause-olni a DAG-ot {new_pipeline.dag_id}: {e}", flush=True)
        return new_pipeline

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def attach_alias(pipeline: ETLConfig):
    if pipeline.schema:
        pipeline.alias = pipeline.schema.alias
    return pipeline

@router.get("/all", response_model=list[ETLConfigResponse])
def get_all_pipelines(db: Session = Depends(get_db)):
    pipelines = (
        db.query(ETLConfig)
        .join(Status, ETLConfig.id == Status.etlconfig_id)
        .filter(
            Status.current_status != "archived",
        )
        .all()
    )
    return [attach_alias(pipeline) for pipeline in pipelines]


@router.post("/load/{pipeline_id}", response_model=ETLConfigResponse)
def load_pipeline_data(
    pipeline_id: int,
    db: Session = Depends(get_db),
):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    return pipeline


@router.post("/updated_pipeline/{pipeline_id}", response_model=ETLConfigResponse)
def updated_pipeline(
        pipeline_id: int,
        config: ETLConfigUpdate,
        db: Session = Depends(get_db),
):
    old_pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()

    if not old_pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    try:
        updated_data = config.dict(exclude_unset=True)
        updated_data['source'] = old_pipeline.source
        new_version = old_pipeline.version + 1

        base_pipeline_name = remove_version_suffix(old_pipeline.pipeline_name)
        new_pipeline_name = f"{base_pipeline_name} v{new_version}"
        new_table_name = generate_table_name(new_pipeline_name, new_version)
        dag_id = generate_dag_id(new_pipeline_name, new_version)

        new_pipeline = ETLConfig(
            **updated_data,
            pipeline_name=new_pipeline_name,
            version=new_version,
            target_table_name=new_table_name,
            dag_id=dag_id,
        )

        db.add(new_pipeline)
        db.flush()
        db.refresh(new_pipeline)

        old_status = db.query(Status).filter(Status.etlconfig_id == pipeline_id).first()
        if old_status:
            old_status.current_status = "archived"
            old_status.updated_at = datetime.utcnow()

        new_status = Status(
            etlconfig_id=new_pipeline.id,
            current_status="queued",
            updated_at=datetime.utcnow()
        )
        db.add(new_status)
        db.commit()
        pause_airflow_dag(old_pipeline.dag_id)
        unpause_airflow_dag(new_pipeline.dag_id)
        return new_pipeline

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Pipeline update failed: {str(e)}")
