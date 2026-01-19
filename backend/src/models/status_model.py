from sqlalchemy import Column, Integer, String, DateTime, Interval, ForeignKey, func
from sqlalchemy.orm import relationship
from .etl_config_model import ETLConfig   # vagy a helyes útvonalon, ha külön package!
from src.database.connection import Base

class Status(Base):
    __tablename__ = 'status'

    etlconfig_id = Column(Integer, ForeignKey('etlconfig.id'), primary_key=True)
    current_status = Column(String(20), nullable=False)
    last_successful_run = Column(DateTime)
    next_scheduled_run = Column(DateTime)
    execution_time = Column(Interval)
    updated_at = Column(DateTime, default=func.current_timestamp())

    # ORM kapcsolat a confighoz – így statusból könnyen eléred a pipeline metaadatait is
    etlconfig = relationship(ETLConfig, backref="status", uselist=False)

    def __repr__(self):
        return (
            f"<Status(etlconfig_id={self.etlconfig_id}, current_status={self.current_status}, "
            f"last_successful_run={self.last_successful_run}, next_scheduled_run={self.next_scheduled_run}, "
            f"execution_time={self.execution_time})>"
        )