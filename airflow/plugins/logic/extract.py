import sqlalchemy as sa
import os
from connector_helper import fetch_data_with_connector, fetch_data_legacy

def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése. A parameters argumentumot használja az indikátorokhoz.
    """
    print(f"[EXTRACT] Starting for Pipeline ID: {pipeline_id}")
    
    # Paraméterek kinyerése a DAG hívásból (op_kwargs)
    parameters = kwargs.get('parameters', {})
    source_type = kwargs.get('source_type')

    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    # Adatbázis ellenőrzése (Schema és Config)
    with engine.connect() as conn:
        # Ha a DAG generátor nem küldött paramétert, megpróbáljuk lekérni DB-ből
        if not parameters:
            print("[EXTRACT] Parameters missing from kwargs, querying DB...")
            p_query = sa.text("SELECT parameters, source FROM etlconfig WHERE id = :id")
            p_result = conn.execute(p_query, {"id": pipeline_id}).mappings().first()
            if p_result:
                parameters = p_result['parameters'] or {}
                source_type = p_result['source']

        # API Schema lekérése a connector típushoz
        schema_query = sa.text("""
                               SELECT s.source,
                                      s.field_mappings,
                                      s.connector_type,
                                      s.endpoint,
                                      s.base_url
                               FROM api_schemas s
                               WHERE s.source = :source
                               """)
        # Figyelem: itt a source_type-ot használjuk a kereséshez
        schema_result = conn.execute(schema_query, {"source": source_type}).mappings().first()

        if not schema_result:
            print(f"[EXTRACT] ⚠️ No API Schema found for source '{source_type}'. Trying legacy mode.")
            # Ha nincs séma, de van URL, legacy mód
            if source_type and (source_type.startswith("http")):
                extracted = fetch_data_legacy(source_type, [])
                kwargs['ti'].xcom_push(key='extracted_data', value=extracted)
                return extracted
            raise Exception(f"No schema found for source {source_type}")

    connector_type = schema_result.get('connector_type')
    endpoint = schema_result.get('endpoint', 'default')
    base_url = schema_result.get('base_url')
    
    # field_mappings betöltése a sémából (ha van)
    field_mappings = schema_result.get('field_mappings')
    if not isinstance(field_mappings, list):
        field_mappings = []

    print(f"[EXTRACT] Connector: {connector_type}, Endpoint: {endpoint}")
    print(f"[EXTRACT] Parameters: {parameters}")

    # --- CONNECTOR INDÍTÁSA ---
    if connector_type:
        try:
            extracted = fetch_data_with_connector(
                connector_type=connector_type,
                endpoint=endpoint,
                parameters=parameters, # <--- Itt megy be a { "indicator": "NCD_BMI_MEAN" }
                base_url=base_url,
                field_mappings=field_mappings
            )
            print(f"[EXTRACT] Success! Records fetched: {len(extracted)}")
        except Exception as e:
            print(f"[EXTRACT] Connector Error: {e}")
            raise e
    else:
        # Ha nincs connector típus definiálva
        print(f"[EXTRACT] Legacy fetch: {source_type}")
        extracted = fetch_data_legacy(source_type, field_mappings)

    # Kulcsok normalizálása (kisbetűsítés)
    def normalize_keys(obj):
        if isinstance(obj, dict):
            return {k.lower(): normalize_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [normalize_keys(v) for v in obj]
        return obj

    normalized = normalize_keys(extracted)

    # XCom Push a következő lépéseknek
    kwargs['ti'].xcom_push(key='extracted_data', value=normalized)
    
    return normalized