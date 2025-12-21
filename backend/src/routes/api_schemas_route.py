from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.models.api_schemas_model import APISchema
from src.schemas.api_schemas_schema import SourceAlias, SourceRequest, APISchemaResponse
import json
from src.services.etl_loader import load_to_target_table
# HIÁNYZÓ IMPORT PÓTOLVA:
from src.connectors.registry import get_connector

router = APIRouter()

@router.post("/load/{pipeline_id}")
def run_pipeline_load(pipeline_id: int, db: Session = Depends(get_db)):
    try:
        load_to_target_table(pipeline_id, db)
        return {"message": "Data loaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/available-sources", response_model=List[SourceAlias])
def get_available_sources(db: Session = Depends(get_db)):
    """Visszaadja az összes elérhető API forrást connector információkkal."""
    sources = db.query(APISchema).all()
    return [
        {
            "source": schema.source,
            "alias": schema.alias,
            "description": schema.description,
            "connector_type": schema.connector_type,
            "config_schema": schema.config_schema
        }
        for schema in sources
    ]


@router.get("/schema/{source}", response_model=APISchemaResponse)
def get_schema_by_source(source: str, db: Session = Depends(get_db)):
    """Visszaadja egy adott forrás teljes sémáját connector információkkal."""
    schema = db.query(APISchema).filter(APISchema.source == source).first()
    if not schema:
        raise HTTPException(status_code=404, detail=f"Schema not found for source: {source}")
    return schema


@router.post("/load-schema")
def load_schema(req: SourceRequest, db: Session = Depends(get_db)):
    normalized_source = req.source.strip().rstrip('/')
    schema = db.query(APISchema).filter(APISchema.source == normalized_source).first()

    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found.")

    # Parse field mappings safely
    field_mappings = schema.field_mappings

    # If stored as text → try to parse JSON
    if isinstance(field_mappings, str):
        try:
            field_mappings = json.loads(field_mappings)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=500,
                detail="field_mappings is not valid JSON string."
            )

    # 🟦 WHO mode: field_mappings is NULL → dynamic schema
    if not field_mappings:
        return {
            "dynamic": True,
            "field_mappings": [],
            "selected_columns": [],
            "column_order": []
        }

    # 🟩 NORMAL static schema mode
    try:
        column_order = [f["name"] for f in field_mappings]
        selected_columns = column_order.copy()
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Invalid field_mappings format. Must be a list of objects with 'name'."
        )

    return {
        "dynamic": False,
        "field_mappings": field_mappings,
        "selected_columns": selected_columns,
        "column_order": column_order
    }


@router.get("/schema/{source}/friendly")
def get_friendly_schema(source: str, db: Session = Depends(get_db)):
    """
    WHO és más dinamikus API-k számára 'friendly' (UI-ready) paraméter leírás.
    Ha nincs külön friendly schema, fallback a rendes schema-ra.
    """
    schema = (
        db.query(APISchema)
        .filter(APISchema.source == source)
        .first()
    )

    if not schema:
        raise HTTPException(
            status_code=404,
            detail=f"Friendly schema not found for source: {source}"
        )

    # Ha van friendly config_schema, visszaadjuk
    if schema.config_schema:
        return schema

    # Ha dynamic, akkor legalább annyit adjunk vissza, hogy paramétereket kell kérni
    if schema.connector_type == "who":
        return {
            "source": schema.source,
            "alias": schema.alias,
            "description": schema.description,
            "connector_type": schema.connector_type,
            "config_schema": schema.config_schema or {},
        }

    # Ha nincs friendly schema → fallback a normálra
    return schema


# --- EZ VOLT A HIÁNYZÓ VÉGPONT ---
@router.get("/connector/{connector_type}/filters")
def get_connector_filters(connector_type: str):
    """
    Visszaadja egy adott connector típushoz (pl. 'who_gho', 'worldbank')
    tartozó szűrő opciókat (pl. indikátorok listája).
    """
    try:
        # 1. Connector példányosítása (paraméterek nélkül, csak a metaadatokért)
        connector = get_connector(connector_type)

        # 2. Szűrők lekérdezése (ez hívja meg a who.py get_filter_options-ét)
        filters = connector.get_filter_options()

        return filters

    except ValueError as e:
        # Ha ismeretlen a connector típus
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Bármilyen egyéb hiba (pl. WHO API hiba)
        print(f"Hiba a connector szűrők lekérésekor: {e}")
        raise HTTPException(status_code=500, detail=str(e))