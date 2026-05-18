import sys
import os
import logging

""" A conncetorok kezeléséhez és az adat lekéréshez szükséges segédfüggvények. """
logger = logging.getLogger(__name__)

# Beállítjuk a plugin-ek útvonalát, hogy az Airflow felismerje a saját moduljainkat.
plugins_path = '/opt/airflow/plugins'
if plugins_path not in sys.path:
    sys.path.insert(0, plugins_path)
# Megpróbáljuk importálni a központi connector register-t.
try:
    from connectors import get_connector

# Ha az elsődleges import nem sikerül, megpróbáljuk a backendből is.
except ImportError as e1:
    try:
        from src.connectors import get_connector
    except ImportError as e2:
        logger.error(f"Error with the connector: {e1}, {e2}")
        get_connector = None

# Adatok lekérése a megadott connector típus, végpont és paraméterek alapján.
def fetch_data_with_connector(connector_type, endpoint, parameters, base_url=None, field_mappings=None):
    if get_connector is None:
        raise ImportError("The get_connector is not available.")

    # A társítjuk a connector típusokat a megfelelő Airflow kapcsolat azonosítókhoz.
    try:
        connection_mapping = {
            "worldbank": "world_bank_api",
            "who": "who_api",
            "f1_api": "f1_api",
            "open_meteo": "open_meteo_api",
            "football_data": "football_data_api",
            "unirate": "unirate_api",
        }
        # Meghatározzuk a kapcsolat azonosítóját és inicializáljuk a connectort.
        conn_id = connection_mapping.get(connector_type.lower(), f"{connector_type.lower()}_api")
        connector = get_connector(connector_type, conn_id=conn_id)
        if base_url and hasattr(connector, 'base_url'):
            connector.base_url = base_url
        with connector:
            data = connector.fetch(endpoint, parameters)

        # Opcionális adatnormalizálás végrehajtása, ha a connector támogatja azt.
        if field_mappings and hasattr(connector, 'normalize_data'):
            data = connector.normalize_data(data, field_mappings)
        return data
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise

# Adatok kinyerése a kapott útvonalból.
def extract_from_path(data, path):
    keys = path.split('.')
    current = data

    # Az elérési útvonal feldarabolása a kezelhetőség érdekében
    for key in keys:
        if '[' in key and ']' in key:
            key_name, idx = key[:-1].split('[')
            current = current.get(key_name, [])
            idx = int(idx)
            if isinstance(current, list) and len(current) > idx:
                current = current[idx]
            else:
                return None
        else:
            if isinstance(current, dict):
                current = current.get(key, None)
            else:
                return None
        if current is None:
            return None
    return current
