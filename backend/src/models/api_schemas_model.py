from sqlalchemy import Column, Integer, String, JSON, DateTime, Text
from datetime import datetime
from src.database.connection import Base


class APISchema(Base):
    __tablename__ = "api_schemas"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, unique=True, nullable=False)
    field_mappings = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    alias = Column(String(255))
    description = Column(Text)
    
    # Connector réteg mezők
    connector_type = Column(String, nullable=True)  # worldbank, undata, oecd, who
    endpoint = Column(String, nullable=True)  # Endpoint azonosító vagy path
    base_url = Column(String, nullable=True)  # Opcionális base URL override
    response_format = Column(String, nullable=True)  # json, csv, xml, excel
    config_schema = Column(JSON, nullable=True)  # UI számára: milyen mezőket kér (pl. {"indicator": "string", "country": "string"})
    auth_profile_id = Column(String, nullable=True)  # Auth profil azonosító (ha később kell)
    pagination_config = Column(JSON, nullable=True)  # Lapozás beállítások (pl. {"type": "offset", "page_size": 1000})


