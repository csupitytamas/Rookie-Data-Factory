import sys
import os
import logging

logger = logging.getLogger(__name__)

# Mivel v1.1-ben a kód bent van a konténerben, fixáljuk az útvonalakat
# Az Airflow a /opt/airflow/plugins mappát automatikusan hozzáadja a path-hoz.
plugins_path = '/opt/airflow/plugins'
if plugins_path not in sys.path:
    sys.path.insert(0, plugins_path)

# Importálás: megpróbáljuk a plugins mappából betölteni a connectors csomagot
try:
    from connectors import get_connector

    logger.info("Siker: get_connector betöltve a connectors csomagból.")
except ImportError as e1:
    try:
        # Alternatív útvonal, ha a backend struktúrából jönne
        from src.connectors import get_connector

        logger.info("Siker: get_connector betöltve a src.connectors csomagból.")
    except ImportError as e2:
        logger.error(f"Kritikus hiba: Nem sikerült importálni a get_connector-t. Hibák: {e1}, {e2}")
        logger.error(f"Jelenlegi sys.path: {sys.path}")
        # Nem állítjuk meg az egész Airflow-t, de jelezzük a hibát
        get_connector = None


def fetch_data_with_connector(connector_type, endpoint, parameters, base_url=None, field_mappings=None):
    if get_connector is None:
        raise ImportError("A get_connector nem elérhető. Ellenőrizd a pluginokat!")

    try:
        connection_mapping = {
            "worldbank": "world_bank_api",
            "oecd": "oecd_api",
            "who": "who_api",
        }

        conn_id = connection_mapping.get(connector_type.lower())
        if not conn_id:
            raise ValueError(f"Unknown connector: {connector_type}")

        connector = get_connector(connector_type, conn_id=conn_id)

        if base_url and hasattr(connector, 'base_url'):
            connector.base_url = base_url

        with connector:
            data = connector.fetch(endpoint, parameters)

        if field_mappings and hasattr(connector, 'normalize_data'):
            data = connector.normalize_data(data, field_mappings)

        return data

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise


def extract_from_path(data, path):
    # (A függvény marad változatlan, ez tökéletesen működik)
    keys = path.split('.')
    current = data
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