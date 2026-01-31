import sqlalchemy as sa
import os
import pandas as pd
from connector_helper import fetch_data_with_connector, fetch_data_legacy

def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése. Kezeli az API-t és a kiegészítő fájlokat (Merge/Concat).
    """
    print(f"[EXTRACT] Starting for Pipeline ID: {pipeline_id}")
    
    # Paraméterek kinyerése a DAG hívásból (op_kwargs)
    parameters = kwargs.get('parameters', {})
    source_type = kwargs.get('source_type')

    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    # 1. PARAMÉTEREK ÉS SÉMA BETÖLTÉSE
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
    
    field_mappings = schema_result.get('field_mappings')
    if not isinstance(field_mappings, list):
        field_mappings = []

    print(f"[EXTRACT] Connector: {connector_type}, Endpoint: {endpoint}")
    print(f"[EXTRACT] Parameters: {parameters}")

    # 2. API ADATOK LEKÉRÉSE
    extracted = []
    if connector_type:
        try:
            extracted = fetch_data_with_connector(
                connector_type=connector_type,
                endpoint=endpoint,
                parameters=parameters,
                base_url=base_url,
                field_mappings=field_mappings
            )
            print(f"[EXTRACT] API fetch success! Records: {len(extracted)}")
        except Exception as e:
            print(f"[EXTRACT] Connector Error: {e}")
            # Hiba esetén döntés: megálljunk vagy engedjük tovább csak a fájllal?
            # Jelenleg tovább engedjük üres listával, ha a fájl miatt jöttünk
            if not parameters.get('extra_file_path'):
                 raise e
            print("[EXTRACT] Continuing with potential file data despite API error.")
    else:
        print(f"[EXTRACT] Legacy fetch: {source_type}")
        extracted = fetch_data_legacy(source_type, field_mappings)

    # 3. KIEGÉSZÍTŐ FÁJL KEZELÉSE ÉS ÖSSZEFÉSÜLÉS (SMART MERGE)
    extra_file_path = parameters.get('extra_file_path')
    
    if extra_file_path and os.path.exists(extra_file_path):
        print(f"[MERGE] Processing extra file: {extra_file_path}")
        
        try:
            # Pandas DataFrames létrehozása az API adatokból
            api_df = pd.DataFrame(extracted)
            
            # Fájl beolvasása kiterjesztés alapján
            ext = os.path.splitext(extra_file_path)[1].lower()
            file_df = pd.DataFrame()
            
            if ext == '.csv': file_df = pd.read_csv(extra_file_path)
            elif ext == '.json': file_df = pd.read_json(extra_file_path)
            elif ext == '.parquet': file_df = pd.read_parquet(extra_file_path)

            if not file_df.empty:
                # Van közös oszlop?
                common_cols = list(set(api_df.columns) & set(file_df.columns))
                
                # Kulcs prioritási lista (hogy ne véletlenszerűen válasszon, ha több van)
                preferred_keys = ['id', 'uuid', 'code', 'iso_code', 'date', 'year', 'country_code']
                join_key = next((c for c in preferred_keys if c in common_cols), common_cols[0] if common_cols else None)

                if join_key:
                    # ESET A: OUTER JOIN (Van közös kulcs)
                    # Adatgazdagítás: sorok egyesítése + új sorok felvétele
                    # how='outer': Minden adat megmarad mindkét forrásból
                    print(f"[MERGE] Strategy: Outer Join on key '{join_key}'")
                    merged_df = pd.merge(api_df, file_df, on=join_key, how='outer')
                else:
                    # ESET B: CONCAT (Nincs közös kulcs)
                    # Adatok hozzáadása: sorok egymás alá fűzése, oszlopok bővítése
                    print(f"[MERGE] Strategy: Concat (No common key found)")
                    merged_df = pd.concat([api_df, file_df], ignore_index=True)

                # NaN értékek cseréje None-ra (JSON kompatibilitás miatt fontos)
                merged_df = merged_df.where(pd.notnull(merged_df), None)
                
                # Eredmény visszaírása az extracted változóba
                extracted = merged_df.to_dict(orient='records')
                print(f"[MERGE] Success. Final record count: {len(extracted)}")
            else:
                print("[MERGE] Warning: Extra file was empty.")

        except Exception as e:
            print(f"[MERGE ERROR] Failed to merge file: {e}")
            # Hiba esetén nem állítjuk meg a folyamatot, marad az eredeti API adat
            pass
    elif extra_file_path:
        print(f"[MERGE] Warning: File path found in params but file does not exist: {extra_file_path}")

    # 4. NORMALIZÁLÁS ÉS OUTPUT
    def normalize_keys(obj):
        if isinstance(obj, dict):
            return {k.lower(): normalize_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [normalize_keys(v) for v in obj]
        return obj

    normalized = normalize_keys(extracted)

    # XCom Push a következő lépéseknek (Transform/Load)
    kwargs['ti'].xcom_push(key='extracted_data', value=normalized)
    
    return normalized