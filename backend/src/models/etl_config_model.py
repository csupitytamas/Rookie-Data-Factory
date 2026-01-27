from sqlalchemy import Column, Integer, String, DateTime, JSON
from datetime import datetime
from src.database.connection import Base
from sqlalchemy.orm import relationship, foreign

class ETLConfig(Base):
    __tablename__ = "etlconfig"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_name = Column(String, nullable=False)
    source = Column(String, nullable=False)
    schedule = Column(String, nullable=False)
    custom_time = Column(String, nullable=True)
    condition = Column(String, nullable=True)
    
    # Fájl feltöltéses mezők
    uploaded_file_name = Column(String, nullable=True)
    uploaded_file_path = Column(String, nullable=True)
    file_format = Column(String, nullable=True) # Új mező a fájl formátumhoz (csv, excel, json)

    dependency_pipeline_id = Column(String, nullable=True)
    update_mode = Column(String, nullable=False) # append, replace, merge
    save_option = Column(String, nullable=False) # todatabase, tofile

    # Komplex konfigurációs mezők (JSON)
    field_mappings = Column(JSON, nullable=True)
    transformation = Column(JSON, nullable=True)
    selected_columns = Column(JSON, nullable=True)
    column_order = Column(JSON, nullable=True)
    group_by_columns = Column(JSON, nullable=True)

    order_by_column = Column(String, nullable=True)
    order_direction = Column(String, nullable=True)
    custom_sql = Column(String, nullable=True)

    # Verziókezelés és Airflow
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    target_table_name = Column(String, nullable=True)
    dag_id = Column(String, nullable=True)
    
    # Connector paraméterek (pl. API kulcsok, ország kódok) - JSON-ben tárolva
    parameters = Column(JSON, nullable=True)

    schema = relationship(
        "APISchema",
        primaryjoin="ETLConfig.source == foreign(APISchema.source)",
        uselist=False,
        lazy="joined"
    )