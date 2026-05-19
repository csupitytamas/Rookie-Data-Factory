import sys
import os
from src.database.connection import engine, Base, SessionLocal
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.models.status_model import Status
from src.models.settings_model import Settings

""" Ez a szkript felelős az adatbázis táblák létrehozásáért (init_db) telepítés során és az alapértelmezett API sémák feltöltéséért (seed_api_schemas). """

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

# Feltölti az adatbázist az implementált API sémákkal.
def seed_api_schemas():
    db = SessionLocal()
    added_count = 0
    try:
        default_schemas = [
            APISchema(
                source="WHO",
                field_mappings=[],
                alias="World Health Organization (GHO)",
                description="WHO Global Health Observatory data",
                connector_type="who",
                endpoint="Indicator",
                base_url="https://ghoapi.azureedge.net/api",
                response_format="json",
                config_schema={
                    "indicator": {
                        "type": "select",
                        "label": "Indicator",
                        "required": True,
                        "description": "Select WHO GHO indicator code"
                    }
                }
            ),
            APISchema(
                source="f1_api",
                field_mappings=[],
                alias="Formula-1 Data API",
                description="Official F1 race, lap time and telemetry data",
                connector_type="f1_api",
                endpoint="sessions",
                base_url="https://api.openf1.org/v1",
                response_format="json",
                config_schema={}
            ),
            APISchema(
                source="open_meteo",
                field_mappings=[],
                alias="Open-Meteo Weather API",
                description="Free weather forecast and historical data",
                connector_type="open_meteo",
                endpoint="forecast",
                base_url="https://api.open-meteo.com/v1",
                response_format="json",
                config_schema={}
            ),
            APISchema(
                source="football_data",
                field_mappings=[],
                alias="Football Data API",
                description="Leagues, matches and standings from football-data.org",
                connector_type="football_data",
                endpoint="matches",
                base_url="https://api.football-data.org/v4",
                response_format="json",
                config_schema={}
            ),
            APISchema(
                source="unirate",
                field_mappings=[],
                alias="UniRate Currency API",
                description="Real-time and historical currency exchange rates",
                connector_type="unirate",
                endpoint="rates",
                base_url="https://api.unirateapi.com/api",
                response_format="json",
                config_schema={}
            )
        ]

        # Meghatározzuk az aktuális sémákat, és eltávolítjuk azokat, amelyek már nincsenek a listában.
        current_sources = [s.source for s in default_schemas]
        db.query(APISchema).filter(APISchema.source.notin_(current_sources)).delete(synchronize_session=False)

        # Végigmegyünk az alapértelmezett sémákon: frissítjük a meglévőket vagy hozzáadjuk az újakat.
        for schema in default_schemas:
            existing = db.query(APISchema).filter(APISchema.source == schema.source).first()
            if existing:
                existing.alias = schema.alias
                existing.description = schema.description
                existing.connector_type = schema.connector_type
                existing.base_url = schema.base_url
                existing.config_schema = schema.config_schema
            else:
                db.add(schema)
                added_count += 1
        db.commit()
        print(f"Success! API schemas synchronized. (New: {added_count}, Total: {len(current_sources)})")
    except Exception as e:
        print(f"Error loading initial data: {e}")
        db.rollback()
    finally:
        db.close()

# Inicializálja az adatbázist: létrehozza a táblákat és meghívja a seed funkciót.
def init_db():
    print("Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        print("Success! Static tables (users, user_sessions, etlconfig, api_schemas, status) created.")
        seed_api_schemas()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    init_db()
