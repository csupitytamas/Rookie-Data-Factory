import sys
import os

# Hozzáadjuk a parent directory-t az útvonalhoz (három szintet lépünk vissza a src/database-ből)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

# --- FONTOS JAVÍTÁS: Importáljuk a SessionLocal-t is az íráshoz ---
from src.database.connection import engine, Base, SessionLocal

# Modellek importálása
from src.models.users_model import User
from src.models.auth_model import UserSession
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.models.status_model import Status

# Kapcsolatok betöltése
import src.models.relationships

def seed_api_schemas():
    """Alapértelmezett API sémák betöltése a jelenlegi modellezés alapján, ha a tábla még üres."""
    db = SessionLocal()
    try:
        # Ellenőrizzük, hogy van-e már legalább egy séma
        if db.query(APISchema).count() == 0:
            print("Üres az api_schemas tábla. Alapértelmezett adatok betöltése...")
            
            # Itt definiáljuk az alap API-kat a CSV sorok alapján
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
                    source="NBA",
                    field_mappings=[],
                    alias="NBA Statistics",
                    description="NBA players and teams data via BallDontLie API",
                    connector_type="nba",
                    endpoint="teams",
                    base_url="https://api.balldontlie.io/v1",
                    response_format="json",
                    config_schema={
                        "team_id": {
                            "type": "select",
                            "label": "NBA Csapat",
                            "required": False,
                            "description": "Valassz ki egy csapatot a listabol (hagyd uresen az osszeshez)",
                            "options": []
                        }
                    }
                ),
                APISchema(
                    source="OECD",
                    field_mappings=[],
                    alias="OECD Data Explorer",
                    description="OECD statisztikai adatok (Uj SDMX 3.0 API)",
                    connector_type="oecd",
                    endpoint="",
                    base_url="https://sdmx.oecd.org/public/rest",
                    response_format="json",
                    config_schema={
                        "indicator": {
                            "type": "select",
                            "label": "Organisation for Economic Co-operation and Development API (OECD)",
                            "required": True,
                            "description": "Select from the list"
                        }
                    }
                )
            ]
            
            # Hozzáadjuk és elmentjük őket az adatbázisba
            db.add_all(default_schemas)
            db.commit()
            print("Siker! A WHO, NBA és OECD sémák tökéletes formátumban betöltve.")
        else:
            print("Az API sémák már léteznek, nincs szükség betöltésre.")
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