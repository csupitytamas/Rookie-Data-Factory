from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class APISchemaField(BaseModel):
    name: str
    type: str

class APISchemaBase(BaseModel):
    source: str
    field_mappings: Optional[List[APISchemaField]] = None
    alias: str | None = None
    description: str | None = None
    connector_type: Optional[str] = None  # worldbank, undata, oecd, who
    endpoint: Optional[str] = None
    base_url: Optional[str] = None
    response_format: Optional[str] = None  # json, csv, xml, excel
    config_schema: Optional[Dict[str, Any]] = None  # UI számára: milyen mezőket kér
    auth_profile_id: Optional[str] = None
    pagination_config: Optional[Dict[str, Any]] = None

class SourceRequest(BaseModel):
    source: str
    parameters: Optional[Dict[str, Any]] = {}
    
class SourceAlias(BaseModel):
    source: str
    alias: str | None = None
    description: str | None = None
    connector_type: Optional[str] = None
    config_schema: Optional[Dict[str, Any]] = None

    model_config = {
        "from_attributes": True}


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



