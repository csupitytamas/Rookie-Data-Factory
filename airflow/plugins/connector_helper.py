import sys
import os
import logging

logger = logging.getLogger(__name__)

possible_paths = [
    '/opt/backend/src',
    os.path.join(os.path.dirname(__file__), '..', '..', 'backend', 'src'),
    os.path.join(os.path.dirname(__file__), '..', '..', '..', 'backend', 'src'),
]

for backend_path in possible_paths:
    if os.path.exists(backend_path) and backend_path not in sys.path:
        sys.path.insert(0, backend_path)
        break

try:
    from connectors import get_connector
except ImportError:
    try:
        from src.connectors import get_connector
    except ImportError:
        pass


def fetch_data_with_connector(connector_type, endpoint, parameters, base_url=None, field_mappings=None):
    try:
        connection_mapping = {
            "worldbank": "world_bank_api",
            "oecd": "oecd_api",
            "who": "who_api",
            "undata": "un_data_api"
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