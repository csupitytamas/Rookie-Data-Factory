import sys
import os

# Hozzáadjuk a parent directory-t az útvonalhoz (három szintet lépünk vissza a src/database-ből)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

# --- FONTOS JAVÍTÁS: Importáljuk a SessionLocal-t is az íráshoz ---
from src.database.connection import engine, Base, SessionLocal

# Modellek importálása
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.models.status_model import Status
from src.models.settings_model import Settings


def seed_api_schemas():
    """Alapértelmezett API sémák betöltése a jelenlegi modellezés alapján."""
    db = SessionLocal()
    added_count = 0
    try:
        # Itt definiáljuk az összes API-t (a meglévőket és az újat is)
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
            # --- AZ ÚJ FORMA-1 API ---
            APISchema(
                source="f1_api",  # Fontos: egyeznie kell a registry.py-ban lévő névvel!
                field_mappings=[],
                alias="Formula-1 Data API",
                description="F1 hivatalos verseny, köridő és telemetriai adatok",
                connector_type="f1_api",
                endpoint="sessions",
                base_url="https://api.openf1.org/v1",
                response_format="json",
                config_schema={}  # Ezt üresen is hagyhatod, a connector .get_filter_options() adja a frontendnek
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

        # Végigmegyünk a listán, és csak azt adjuk hozzá, ami még nincs a táblában
        # VAGY frissítjük, ha már létezik
        current_sources = [s.source for s in default_schemas]
        
        # 1. TÖRLÉS: Ami az adatbázisban benne van, de a listánkban nincs, azt töröljük
        db.query(APISchema).filter(APISchema.source.notin_(current_sources)).delete(synchronize_session=False)

        # 2. FELVITEL / FRISSÍTÉS:
        for schema in default_schemas:
            existing = db.query(APISchema).filter(APISchema.source == schema.source).first()
            if existing:
                # Frissítjük a meglévőt (pl. ha változott az alias vagy a connector_type)
                existing.alias = schema.alias
                existing.description = schema.description
                existing.connector_type = schema.connector_type
                existing.base_url = schema.base_url
                existing.config_schema = schema.config_schema
            else:
                db.add(schema)
                added_count += 1

        db.commit()
        print(f"Siker! Az API sémák szinkronizálva. (Új: {added_count}, Összes: {len(current_sources)})")

    except Exception as e:
        print(f"Hiba a kezdőadatok betöltésekor: {e}")
        db.rollback()
    finally:
        db.close()


def init_db():
    print("Adatbázis táblák létrehozása...")
    try:
        # Létrehozza a táblákat
        Base.metadata.create_all(bind=engine)
        print("Siker! A statikus táblák (users, user_sessions, etlconfig, api_schemas, status) létrejöttek.")

        # --- FONTOS JAVÍTÁS: Itt HÍVJUK MEG a feltöltő függvényt ---
        seed_api_schemas()

    except Exception as e:
        print(f"Hiba történt: {e}")

if __name__ == "__main__":
    init_db()
