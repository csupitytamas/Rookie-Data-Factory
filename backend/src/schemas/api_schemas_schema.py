from pydantic import BaseModel
from typing import List, Optional, Dict, Any

""" A modul definiálja az API sémákhoz kapcsolódó sémákat, amelyeket az adatok validálására és a kérések/válaszok strukturálására használja fel. """

# Egyetlen API mező leírását reprezentáló modell (név és típus).
class APISchemaField(BaseModel):
    name: str
    type: str

# Az API sémák alapvető tulajdonságait tartalmazó alapmodell.
class APISchemaBase(BaseModel):
    source: str
    field_mappings: Optional[List[APISchemaField]] = None
    alias: str | None = None
    description: str | None = None
    connector_type: Optional[str] = None
    endpoint: Optional[str] = None
    base_url: Optional[str] = None
    response_format: Optional[str] = None
    config_schema: Optional[Dict[str, Any]] = None
    auth_profile_id: Optional[str] = None
    pagination_config: Optional[Dict[str, Any]] = None

# Forrás lekérdezéséhez használt kérés modell, amely a forrás nevét és opcionális paramétereket vár.
class SourceRequest(BaseModel):
    source: str
    parameters: Optional[Dict[str, Any]] = {}

# Egy forrás rövidített változatát reprezentáló modell a listázáshoz.
class SourceAlias(BaseModel):
    source: str
    alias: str | None = None
    description: str | None = None
    connector_type: Optional[str] = None
    config_schema: Optional[Dict[str, Any]] = None

    model_config = {
        "from_attributes": True}

# Az API sémák teljes adatait tartalmazó válaszmodell.
class APISchemaResponse(APISchemaBase):
    id: int
    connector_type: Optional[str] = None
    endpoint: Optional[str] = None
    base_url: Optional[str] = None
    response_format: Optional[str] = None
    config_schema: Optional[Dict[str, Any]] = None
    auth_profile_id: Optional[str] = None
    pagination_config: Optional[Dict[str, Any]] = None

    model_config = {
        "from_attributes": True}



