import sys
import os

# Hozzáadjuk a parent directory-t az útvonalhoz
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import engine, Base

# --- JAVÍTOTT IMPORTOK ---
from src.models.users_model import User
from src.models.auth_model import UserSession  # Itt volt a hiba: Auth -> UserSession
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.models.status_model import Status

# A kapcsolatok betöltése (fontos, hogy a relationship-ek létrejöjjenek)
import src.models.relationships

def init_db():
    print("Adatbázis táblák létrehozása...")
    try:
        # Létrehozza a táblákat az importált modellek alapján
        Base.metadata.create_all(bind=engine)
        print("Siker! A statikus táblák (users, user_sessions, etlconfig, api_schemas, status) létrejöttek.")
    except Exception as e:
        print(f"Hiba történt: {e}")

if __name__ == "__main__":
    init_db()