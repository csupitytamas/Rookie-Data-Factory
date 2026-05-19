from sqlalchemy import Column, Integer, String, JSON, DateTime, Text
from datetime import datetime
from src.database.connection import Base

""" 
A modell definiálja az API sémák metaadatait, beleértve a forrásadatokat, 
mezőleképezéseket és a connectorok konfigurációit.
"""

class APISchema(Base):
    __tablename__ = "api_schemas"
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, unique=True, nullable=False)
    field_mappings = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    alias = Column(String(255))
    description = Column(Text)
    connector_type = Column(String, nullable=True)
    endpoint = Column(String, nullable=True)
    base_url = Column(String, nullable=True)
    response_format = Column(String, nullable=True)
    config_schema = Column(JSON, nullable=True)
    auth_profile_id = Column(String, nullable=True)
    pagination_config = Column(JSON, nullable=True)


