from sqlalchemy import Column, Integer, String
from src.database.connection import Base

class Settings(Base):
    __tablename__ = "system_settings"

    id = Column(Integer, primary_key=True, index=True)
    timezone = Column(String, default="Europe/Budapest")
    download_path = Column(String, nullable=True)