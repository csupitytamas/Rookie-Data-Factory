from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from src.schemas import  ETLConfigBase, ETLConfigUpdate, ETLConfigResponse
from src.database.connection import get_db
from src.models.etl_config_model import ETLConfig
from src.models.status_model import Status
from src.models.api_schemas_model import APISchema
from src.utils.db_creation_util import generate_table_name, generate_dag_id, remove_version_suffix
from src.common.airflow_client import pause_airflow_dag, unpause_airflow_dag, trigger_airflow_dag_with_retry
from datetime import datetime
import shutil
import os
import pandas as pd
from fastapi import UploadFile, File
from sqlalchemy import inspect
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks

"""
A modul az pipeline-ok létrehozását, frissítését, listázását, valamint a hozzájuk kapcsolódó kiegészítő fájlok feltöltését végző HTT(CRUD) végpontokat tartalmazza.
"""

router = APIRouter()

# Új pipeline létrehozása, a konfiguráció mentése és az Airflow szinkronizáció elindítása a háttérben.
@router.post("/create", response_model=ETLConfigResponse)
def create_pipeline(
    config: ETLConfigBase,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    ):

    # Generáljuk az egyedi céltábla nevet és az Airflow DAG ID-t, majd elmentjük az új konfigurációt.
    try:
        pipeline_data = config.model_dump()
        table_name = generate_table_name(config.pipeline_name, version=1)
        dag_id = generate_dag_id(config.pipeline_name, version=1)
        pipeline_data["target_table_name"] = table_name
        pipeline_data["dag_id"] = dag_id
        new_pipeline = ETLConfig(**pipeline_data, version=1)

        # Az új pipeline mentése az adatbázisba.
        db.add(new_pipeline)
        db.commit()
        db.refresh(new_pipeline)

        # Az Airflow metaadatok frissítése és az új pipeline futtatásának elindítása a háttérben.
        background_tasks.add_task(trigger_airflow_dag_with_retry, "refresh_pipeline_metadata")
        background_tasks.add_task(trigger_airflow_dag_with_retry, new_pipeline.dag_id)
        background_tasks.add_task(trigger_airflow_dag_with_retry, "status_sync_dag")
        return new_pipeline
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# Segédfüggvény: Hozzárendeli a séma alapján megadott alias-t a pipeline objektumhoz.
def attach_alias(pipeline: ETLConfig):
    if pipeline.schema:
        pipeline.alias = pipeline.schema.alias
    return pipeline


# Lekéri az összes aktív (nem archivált) pipeline-t az adatbázisból.
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

# Betölti egy meglévő pipeline konfigurációs adatait.
@router.post("/load/{pipeline_id}", response_model=ETLConfigResponse)
def load_pipeline_data(
    pipeline_id: int,
    db: Session = Depends(get_db),
):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline

# Meglévő pipeline frissítése: új verziót hoz létre a régi archiválása mellett.
@router.post("/updated_pipeline/{pipeline_id}", response_model=ETLConfigResponse)
def updated_pipeline(
        pipeline_id: int,
        config: ETLConfigUpdate,
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db),
):
    old_pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not old_pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Kinyerjük a frissített adatokat, eltávolítjuk a rendszer által kezelt mezőket, és generáljuk az új verziós neveket.
    try:
        updated_data = config.model_dump()
        keys_to_remove = ["pipeline_name", "version", "target_table_name", "dag_id", "id", "created_at", "modified_at"]
        for key in keys_to_remove:
            updated_data.pop(key, None)
        updated_data['source'] = old_pipeline.source
        new_version = old_pipeline.version + 1
        base_pipeline_name = remove_version_suffix(old_pipeline.pipeline_name)
        new_pipeline_name = f"{base_pipeline_name} v{new_version}"
        new_table_name = generate_table_name(new_pipeline_name, new_version)
        dag_id = generate_dag_id(new_pipeline_name, new_version)

        # Létrehozzuk az új pipeline entitást a frissített adatokkal és új azonosítókkal.
        new_pipeline = ETLConfig(
            **updated_data,
            pipeline_name=new_pipeline_name,
            version=new_version,
            target_table_name=new_table_name,
            dag_id=dag_id,
        )

        # Mentjük az új verziót.
        db.add(new_pipeline)
        db.flush()
        db.refresh(new_pipeline)

        # Archiváljuk a régi verzióhoz tartozó állapotot.
        old_status = db.query(Status).filter(Status.etlconfig_id == pipeline_id).first()
        if old_status:
            old_status.current_status = "archived"
            old_status.updated_at = datetime.now()

        # Beállítjuk az új verzió kezdeti állapotát (queued).
        new_status = Status(
            etlconfig_id=new_pipeline.id,
            current_status="queued",
            updated_at=datetime.now()
        )
        db.add(new_status)
        db.commit()

        # Leállítjuk a régi DAG-ot az Airflow-ban, majd frissítjük a metadatákat és elindítjuk az új pipeline szinkronizációját.
        try:
            pause_airflow_dag(old_pipeline.dag_id)
            background_tasks.add_task(trigger_airflow_dag_with_retry, "refresh_pipeline_metadata")
            background_tasks.add_task(trigger_airflow_dag_with_retry, new_pipeline.dag_id)
            background_tasks.add_task(trigger_airflow_dag_with_retry, "status_sync_dag")
        except Exception as e:
            print(f"[WARNING] Airflow triggers failed: {e}")
        return new_pipeline
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Pipeline update failed: {str(e)}")

# Feltölt egy kiegészítő fájlt, amelyet a pipeline adatok összekapcsolásához használhat fel.
@router.post("/upload-extra-file")
async def upload_extra_file(file: UploadFile = File(...)):

    # Létrehozzuk a megosztott feltöltési könyvtárat a backendben, amely később az Airflow által is olvasható lesz.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    upload_dir = os.path.join(src_dir, "shared_uploads")
    os.makedirs(upload_dir, exist_ok=True)
    physical_path = os.path.join(upload_dir, file.filename)

    # Elmentjük a fájlt, majd beolvassuk az első sorát, hogy visszatérhessünk a benne található oszlopnevekkel.
    try:
        with open(physical_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        ext = os.path.splitext(file.filename)[1].lower()
        if ext == '.csv':
            df = pd.read_csv(physical_path, nrows=1)
        elif ext == '.json':
            df = pd.read_json(physical_path).head(1)
        elif ext == '.parquet':
            df = pd.read_parquet(physical_path).head(1)
        else:
            return {"error": "Unsupported format"}

        # Visszaadjuk a fájl Airflow konténerből elérhető belső elérési útját és az oszlopneveket.
        airflow_path = f"/opt/airflow/plugins/shared_uploads/{file.filename}"
        return {
            "message": "Succes",
            "file_path": airflow_path, 
            "columns": df.columns.tolist()
        }
    except Exception as e:
        return {"error": str(e)}

# Lekéri egy adott pipeline céltáblájának oszlopneveit, amennyiben a tábla már létezik.
@router.get("/{pipeline_id}/columns")
def get_pipeline_columns(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(ETLConfig).filter(ETLConfig.id == pipeline_id).first()
    if not pipeline or not pipeline.target_table_name:
        return []

    # SQLAlchemy inspector a tábla létezésének és oszlopainak ellenőrzésére.
    try:
        inspector = inspect(db.get_bind())
        if inspector.has_table(pipeline.target_table_name):
            return [col['name'] for col in inspector.get_columns(pipeline.target_table_name)]
        return []
    except Exception as e:
        print(f"Error fetching columns: {e}")
        return []