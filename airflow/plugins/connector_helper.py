import sys
import os
import logging

logger = logging.getLogger(__name__)

# Add backend src to path so we can import connectors
# Try multiple paths for different environments
possible_paths = [
    '/opt/backend/src',  # Docker environment (mounted volume)
    os.path.join(os.path.dirname(__file__), '..', '..', 'backend', 'src'),  # Local development
    os.path.join(os.path.dirname(__file__), '..', '..', '..', 'backend', 'src'),  # Alternative local path
]

for backend_path in possible_paths:
    if os.path.exists(backend_path) and backend_path not in sys.path:
        sys.path.insert(0, backend_path)
        break

try:
    from connectors import get_connector
except ImportError as e:
    # Fallback: try importing from src.connectors
    try:
        from src.connectors import get_connector
    except ImportError:
        raise ImportError(
            f"Could not import connectors. Tried paths: {possible_paths}. "
            f"Original error: {e}"
        )


def fetch_data_with_connector(
    connector_type: str,
    endpoint: str,
    parameters: dict,
    base_url: str = None,  # Visszafelé kompatibilitás miatt maradhat a szignatúrában, de már nem ezt használjuk
    field_mappings: list = None
):
    """
    Adatok lekérése Airflow HttpHook-alapú connector használatával.
    
    Args:
        connector_type: Connector típus (worldbank, undata, oecd, who)
        endpoint: Endpoint azonosító
        parameters: Paraméterek dict-je (API-specifikus formátumban, pl. {"indicator": "SP.POP.TOTL"})
        base_url: Opcionális base URL override (Elavult, Connection ID-t használunk helyette)
        field_mappings: Opcionális field mappings a normalizáláshoz
        
    Returns:
        Lista dict-ekkel, normalizált adatok
    """
    try:
        # 1. Connector típus leképezése Airflow Connection ID-ra
        connection_mapping = {
            "worldbank": "world_bank_api",
            "oecd": "oecd_api",
            "who": "who_api",
            "undata": "un_data_api"
        }
        
        conn_id = connection_mapping.get(connector_type.lower())
        if not conn_id:
            raise ValueError(f"Ismeretlen connector típus vagy hiányzó Airflow Connection leképezés: {connector_type}")

        # 2. Connector inicializálása a conn_id-val
        # A base_url helyett már a conn_id paramétert adjuk át
        connector = get_connector(connector_type, conn_id=conn_id)
        
        # Paraméterek logolása
        logger.info(
            f"[Connector] Using Hook-based connector '{connector_type}' (Conn ID: {conn_id}) "
            f"with parameters: {parameters}"
        )
        
        # 3. Adatok lekérése
        with connector:
            data = connector.fetch(endpoint, parameters)
        
        # 4. Normalizálás field_mappings alapján (ha van)
        if field_mappings:
            data = connector.normalize_data(data, field_mappings)
        
        logger.info(f"[Connector] Fetched {len(data)} records")
        return data
        
    except Exception as e:
        logger.error(f"[Connector] Error fetching data: {e}", exc_info=True)
        raise


def fetch_data_legacy(source_url: str, field_mappings: list):
    """
    Régi módszer: közvetlen URL lekérés (backward compatibility).
    
    Args:
        source_url: A teljes API URL
        field_mappings: Field mappings lista
        
    Returns:
        Lista dict-ekkel, extracted adatok
    """
    import requests
    
    logger.info(f"[Legacy] Fetching data from URL: {source_url}")
    
    response = requests.get(source_url)
    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code}")
    
    data = response.json()
    if not isinstance(data, list):
        data = [data]
    
    # Field mapping alkalmazása
    extracted = []
    for item in data:
        record = {}
        for field in field_mappings:
            field_name = field.get('name')
            field_path = field.get('path', field_name)
            value = extract_from_path(item, field_path)
            record[field_name] = value
        extracted.append(record)
    
    logger.info(f"[Legacy] Extracted {len(extracted)} records")
    return extracted


def extract_from_path(data, path):
    """
    Érték kinyerése nested dict-ből path alapján (pl. "country.value").
    """
    keys = path.split('.')
    current = data
    for key in keys:
        if '[' in key and ']' in key:
            # Array index handling
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