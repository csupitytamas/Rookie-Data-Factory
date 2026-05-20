from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.models.api_schemas_model import APISchema
from src.schemas.api_schemas_schema import SourceAlias, SourceRequest, APISchemaResponse
import json

"""
A modul kezeli az API sémák és connectorok elérhetőségét HTTP(CRUD), valamint a mezőleképezések dinamikus betöltését.
"""

router = APIRouter()

# Lekéri az összes elérhető adatforrást és azok alapvető konfigurációit a rendszerből.
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


# Egy adott forrás részletes sémájának lekérdezése név alapján.
@router.get("/schema/{source}", response_model=APISchemaResponse)
def get_schema_by_source(source: str, db: Session = Depends(get_db)):
    schema = db.query(APISchema).filter(APISchema.source == source).first()
    if not schema:
        raise HTTPException(status_code=404, detail=f"Schema not found for source: {source}")
    return schema

@router.get("/schema/{source}/friendly")
def get_friendly_schema(source: str, db: Session = Depends(get_db)):
    schema = db.query(APISchema).filter(APISchema.source == source).first()
    if not schema:
        raise HTTPException(status_code=404, detail=f"Friendly schema not found for source: {source}")
    return schema

# Betölti a mezőleképezéseket egy forráshoz; ha nincs statikus leképezés, megpróbálja dinamikusan lekérdezni az API-tól.
@router.post("/load-schema")
def load_schema(req: SourceRequest, db: Session = Depends(get_db)):
    normalized_source = req.source.strip().rstrip('/')
    schema = db.query(APISchema).filter(APISchema.source == normalized_source).first()
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found.")
    field_mappings = schema.field_mappings

    # Ha a mezőleképezés JSON string formátumban van az adatbázisban, dekódoljuk objektummá.
    if isinstance(field_mappings, str):
        try:
            field_mappings = json.loads(field_mappings)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Invalid JSON in field_mappings")

    # Ha nincsenek előre definiált mezők, megpróbáljuk dinamikusan kideríteni a sémát a connector segítségével.
    if not field_mappings:
        if not req.parameters or not schema.connector_type:
            raise HTTPException(
                status_code=422,
                detail="Parameters or connector type missing for this source. Please fill in the filters."
            )
        # A megfelelő connector példányosítása és egy mintarekord lekérése a dinamikus séma meghatározásához.
        try:
            from src.connectors.registry import get_connector
            connector = get_connector(schema.connector_type)
            fetch_params = {}
            for k, v in req.parameters.items():
                if isinstance(v, dict) and "value" in v:
                    fetch_params[k] = v["value"]
                else:
                    fetch_params[k] = v
            fetch_params['per_page'] = 1
            sample_data = connector.fetch(endpoint=schema.endpoint or "default", parameters=fetch_params)
            if not sample_data:
                raise ValueError("The API returned no data for the provided parameters. Try different filters or a different season.")

            # A mintarekord alapján legeneráljuk a mezőlistát és a típusokat.
            first_row = sample_data[0]
            dynamic_columns = []
            for key in first_row.keys():
                if key.lower() == 'id':
                    continue
                col_type = "string"
                val = first_row[key]
                if isinstance(val, int): col_type = "int"
                elif isinstance(val, float): col_type = "float"
                dynamic_columns.append({"name": key, "type": col_type, "path": key})
            return {
                "dynamic": True,
                "field_mappings": dynamic_columns,
                "column_order": [f["name"] for f in dynamic_columns]
            }
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"The API is unavailable or the provided parameters are incorrect. Error: {str(e)}")

    # Ha vannak előre definiált mezők, azokat adjuk vissza.
    try:
        column_order = [f["name"] for f in field_mappings]
        return {
            "dynamic": False,
            "field_mappings": field_mappings,
            "column_order": column_order
        }
    except Exception:
        raise HTTPException(status_code=500, detail="Invalid field_mappings format.")

# Lekéri az adott connectorhoz tartozó szűrési opciókat.
@router.get("/connector/{connector_type}/filters")
def get_connector_filters(connector_type: str, request: Request):
    try:
        from src.connectors.registry import get_connector
        connector = get_connector(connector_type)
        params = dict(request.query_params)
        filters = connector.get_filter_options(params)
        return filters
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
