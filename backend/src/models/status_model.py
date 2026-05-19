from sqlalchemy import Column, Integer, String, TIMESTAMP, Interval, ForeignKey, DateTime, func
from sqlalchemy.orm import relationship
from src.models.etl_config_model import ETLConfig
from src.database.connection import Base
from datetime import datetime

""" A pipeline állapotának követésére és előzményeinek tárolására szolgáló modellek. """

# A modell tárolja a pipeline-ok aktuális állapotát és futási statisztikáit.
class Status(Base):
    __tablename__ = 'status'
    etlconfig_id = Column(Integer, ForeignKey('etlconfig.id'), primary_key=True)
    current_status = Column(String(20), nullable=False)
    last_successful_run = Column(DateTime)
    next_scheduled_run = Column(DateTime)
    execution_time = Column(Interval)
    updated_at = Column(DateTime, default=func.current_timestamp())
    etlconfig = relationship("ETLConfig", backref="status", uselist=False)

    # Visszaadja a Status objektum olvasható szöveges reprezentációját.
    def __repr__(self):
        return (
            f"<Status(etlconfig_id={self.etlconfig_id}, current_status={self.current_status}, "
            f"last_successful_run={self.last_successful_run}, next_scheduled_run={self.next_scheduled_run}, "
            f"execution_time={self.execution_time})>"
        )

# Ez a modell tárolja az pipeline-ok állapotváltozásaiat.
class StatusHistory(Base):
    __tablename__ = "status_history"
    id = Column(Integer, primary_key=True, index=True)
    etlconfig_id = Column(Integer)
    status = Column(String)
    changed_at = Column(TIMESTAMP, default=datetime.now)
    execution_time = Column(Interval)
    message = Column(String)
