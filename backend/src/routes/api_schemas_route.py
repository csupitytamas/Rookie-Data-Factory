from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.models.api_schemas_model import APISchema
from src.schemas.api_schemas_schema import SourceAlias, SourceRequest, APISchemaResponse
import json
from src.services.etl_loader import load_to_target_table

router = APIRouter()

# 🟢 Globális alapértelmezett mezők fallback esetére
STANDARD_STAT_FIELDS = [
    {"name": "country", "type": "string", "path": "country"},
    {"name": "year", "type": "string", "path": "year"},
    {"name": "value", "type": "float", "path": "value"},
    {"name": "indicator", "type": "string", "path": "indicator"},
]

@router.post("/load/{pipeline_id}")
def run_pipeline_load(pipeline_id: int, db: Session = Depends(get_db)):
    try:
        load_to_target_table(pipeline_id, db)
        return {"message": "Data loaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/available-sources", response_model=List[SourceAlias])
def get_available_sources(db: Session = Depends(get_db)):
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
    schema = db.query(APISchema).filter(APISchema.source == source).first()
    if not schema:
        raise HTTPException(status_code=404, detail=f"Schema not found for source: {source}")
    return schema

@router.post("/load-schema")
def load_schema(req: SourceRequest, db: Session = Depends(get_db)):
    """
    Dinamikusan betölti a sémát.
    """
    normalized_source = req.source.strip().rstrip('/')
    schema = db.query(APISchema).filter(APISchema.source == normalized_source).first()

    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found.")

    field_mappings = schema.field_mappings
    if isinstance(field_mappings, str):
        try:
            field_mappings = json.loads(field_mappings)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Invalid JSON in field_mappings")

    # 🟦 DINAMIKUS MÓD (Ha a DB-ben NULL a field_mappings)
    if not field_mappings:
        dynamic_columns = []
        
        # 1. Próbáljuk meg a dinamikus lekérdezést
        if req.parameters and schema.connector_type:
            print(f"[Schema Load] Dynamic fetch for {schema.connector_type} with params: {req.parameters}")
            try:
                # 🟢 HELYI IMPORT a körkörös importok elkerülése végett
                from src.connectors.registry import get_connector
                
                connector = get_connector(schema.connector_type)
                
                # Lekérünk 1 darab adatot mintának
                fetch_params = req.parameters.copy()
                fetch_params['per_page'] = 1 
                
                sample_data = connector.fetch(
                    endpoint=schema.endpoint or "default",
                    parameters=fetch_params
                )

                if sample_data and len(sample_data) > 0:
                    first_row = sample_data[0]
                    for key in first_row.keys():
                        col_type = "string"
                        val = first_row[key]
                        if isinstance(val, int):
                            col_type = "int"
                        elif isinstance(val, float):
                            col_type = "float"
                        
                        dynamic_columns.append({
                            "name": key,
                            "type": col_type,
                            "path": key
                        })
                    print(f"[Schema Load] Successfully inferred {len(dynamic_columns)} columns.")
                else:
                    print("[Schema Load] No data returned from connector sample fetch.")
            
            except Exception as e:
                print(f"⚠️ Hiba a dinamikus séma felderítésekor: {e}")
                # Nem állunk meg, megyünk a fallback-re

        # 2. Fallback: Ha a dinamikus rész nem sikerült vagy nem volt adat
        if not dynamic_columns:
             print("[Schema Load] Falling back to standard static fields.")
             dynamic_columns = list(STANDARD_STAT_FIELDS)
             
             if schema.connector_type == 'who':
                 dynamic_columns.append({"name": "who_id", "type": "int", "path": "id"})

        # 🟢 BIZTOSÍTOTT RETURN: Ez mindig lefut a dinamikus ágon
        return {
            "dynamic": True,
            "field_mappings": dynamic_columns,
            "selected_columns": [f["name"] for f in dynamic_columns],
            "column_order": [f["name"] for f in dynamic_columns]
        }

    # 🟩 STATIKUS MÓD
    try:
        column_order = [f["name"] for f in field_mappings]
        return {
            "dynamic": False,
            "field_mappings": field_mappings,
            "selected_columns": column_order,
            "column_order": column_order
        }
    except Exception:
        raise HTTPException(status_code=500, detail="Invalid field_mappings format.")

@router.get("/schema/{source}/friendly")
def get_friendly_schema(source: str, db: Session = Depends(get_db)):
    schema = db.query(APISchema).filter(APISchema.source == source).first()
    if not schema:
        raise HTTPException(status_code=404, detail=f"Friendly schema not found for source: {source}")

    if schema.config_schema:
        return schema

    if schema.connector_type == "who":
        return {
            "source": schema.source,
            "alias": schema.alias,
            "description": schema.description,
            "connector_type": schema.connector_type,
            "config_schema": schema.config_schema or {},
        }
    return schema

@router.get("/connector/{connector_type}/filters")
def get_connector_filters(connector_type: str):
    try:
        from src.connectors.registry import get_connector
        connector = get_connector(connector_type)
        filters = connector.get_filter_options()
        return filters
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Hiba a connector szűrők lekérésekor: {e}")
        raise HTTPException(status_code=500, detail=str(e))