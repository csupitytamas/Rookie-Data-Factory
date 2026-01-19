
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
    uploaded_file_name = Column(String, nullable=True)
    uploaded_file_path = Column(String, nullable=True)
    dependency_pipeline_id = Column(String, nullable=True)
    update_mode = Column(String, nullable=False)
    save_option = Column(String, nullable=False)


    field_mappings = Column(JSON, nullable=True)
    transformation = Column(JSON, nullable=True)
    selected_columns = Column(JSON, nullable=True)
    column_order = Column(JSON, nullable=True)
    group_by_columns = Column(JSON, nullable=True)

    order_by_column = Column(String, nullable=True)
    order_direction = Column(String, nullable=True)
    custom_sql = Column(String, nullable=True)

    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    target_table_name = Column(String, nullable=True)
    dag_id = Column(String, nullable=True)
    file_format = Column(String, nullable=True)
    
    # Connector paraméterek - ezeket a UI-ban választják ki (pl. indicator, country, stb.)
    parameters = Column(JSON, nullable=True)  # {"indicator": "SP.POP.TOTL", "country": "USA", "year": 2020}

    schema = relationship(
        "APISchema",
        primaryjoin="ETLConfig.source == foreign(APISchema.source)",
        uselist=False,
        lazy="joined"
    )



