import sqlalchemy as sa
import os
from connector_helper import fetch_data_with_connector, fetch_data_legacy
# JSON path parser
def extract_from_path(data, path):
    print(f"[extract_from_path] START - path: {path}")
    keys = path.split('.')
    current = data
    for i, key in enumerate(keys):
        print(f"  [Level {i}] key: {key} | current type: {type(current).__name__} | value: {str(current)[:80]}")
        if '[' in key and ']' in key:
            key_name, idx = key[:-1].split('[')
            print(f"    -> Array key: {key_name} | idx: {idx}")
            current = current.get(key_name, [])
            idx = int(idx)
            if isinstance(current, list) and len(current) > idx:
                current = current[idx]
            else:
                print("    [extract_from_path] Array index out of range or not a list, returning None.")
                return None
        else:
            if isinstance(current, dict):
                current = current.get(key, None)
                print(f"    -> Dict get: {key} -> {str(current)[:80]}")
            else:
                print("    [extract_from_path] Not a dict, returning None.")
                return None
        if current is None:
            print("    [extract_from_path] Current is None, returning None.")
            return None
    print(f"[extract_from_path] END - value: {current}")
    return current

def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése API-ból connector réteg használatával.
    Ha van connector_type, akkor a connector-öket használja,
    egyébként a régi módszert (backward compatibility).
    """
    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    with engine.connect() as conn:
        # API Schema lekérése
        schema_query = sa.text("""
                               SELECT s.source,
                                      s.field_mappings,
                                      s.connector_type,
                                      s.endpoint,
                                      s.base_url
                               FROM api_schemas s
                               WHERE s.source = (SELECT source FROM etlconfig WHERE id = :id)
                               """)
        schema_result = conn.execute(schema_query, {"id": pipeline_id}).mappings().first()

        if not schema_result:
            raise Exception(f"API schema not found for pipeline {pipeline_id}")

        # Pipeline paraméterek lekérése
        pipeline_query = sa.text("""
                                 SELECT parameters
                                 FROM etlconfig
                                 WHERE id = :id
                                 """)
        pipeline_result = conn.execute(pipeline_query, {"id": pipeline_id}).mappings().first()
        parameters = pipeline_result.get('parameters') if pipeline_result else None

    source = schema_result['source']
    field_mappings = schema_result['field_mappings']
    connector_type = schema_result.get('connector_type')
    endpoint = schema_result.get('endpoint', 'default')
    base_url = schema_result.get('base_url')

    # --- CONNECTOR RÉTEG ---
    if connector_type:
        print(f"[EXTRACT_DATA] Using connector: {connector_type}")
        print(f"[EXTRACT_DATA] Endpoint: {endpoint}")
        print(f"[EXTRACT_DATA] Parameters: {parameters}")

        try:
            extracted = fetch_data_with_connector(
                connector_type=connector_type,
                endpoint=endpoint,
                parameters=parameters or {},
                base_url=base_url,
                field_mappings=field_mappings
            )
            print(f"[EXTRACT_DATA] Successfully extracted {len(extracted)} records using connector")
        except Exception as e:
            print(f"[EXTRACT_DATA] Error with connector: {e}")

            # legacy fallback *csak* ha a source valódi URL
            if source and (source.startswith("http://") or source.startswith("https://")):
                print(f"[EXTRACT_DATA] Falling back to legacy method (source is URL)")
                extracted = fetch_data_legacy(source, field_mappings)
            else:
                raise Exception(
                    f"Connector '{connector_type}' failed and cannot fallback to legacy method "
                    f"(source '{source}' is not a URL). Original error: {e}"
                )

    # --- LEGACY MÓDSZER ---
    else:
        print(f"[EXTRACT_DATA] Using legacy method (direct URL)")
        print(f"[EXTRACT_DATA] Source URL: {source}")
        extracted = fetch_data_legacy(source, field_mappings)

    # ====== KULCS NORMALIZÁLÁS MINDEN ESETBEN ======
    def normalize_keys(obj):
        if isinstance(obj, dict):
            return {k.lower(): normalize_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [normalize_keys(v) for v in obj]
        return obj

    normalized = normalize_keys(extracted)

    # ====== XCOM PUSH ======
    kwargs['ti'].xcom_push(key='extracted_data', value=normalized)
    print(f"[EXTRACT_DATA] Pushed {len(normalized)} normalized records to XCom")

    return normalized
