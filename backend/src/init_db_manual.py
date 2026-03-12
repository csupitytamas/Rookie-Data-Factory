import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import engine, Base
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.models.status_model import Status,StatusHistory
from src.models.settings_model import Settings

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    init_db()