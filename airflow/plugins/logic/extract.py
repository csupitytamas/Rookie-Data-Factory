import sqlalchemy as sa
import os
import pandas as pd
from connector_helper import fetch_data_with_connector, fetch_data_legacy

def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése. Kezeli az API-t és a kiegészítő fájlokat.
    NINCS kisbetűsítés: az oszlopnevek maradnak eredeti formájukban.
    """
    print(f"[EXTRACT] Starting for Pipeline ID: {pipeline_id}")
    
    # Paraméterek kinyerése
    parameters = kwargs.get('parameters', {})
    source_type = kwargs.get('source_type')

    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    # 1. PARAMÉTEREK ÉS SÉMA BETÖLTÉSE
    with engine.connect() as conn:
        if not parameters:
            print("[EXTRACT] Parameters missing from kwargs, querying DB...")
            p_query = sa.text("SELECT parameters, source FROM etlconfig WHERE id = :id")
            p_result = conn.execute(p_query, {"id": pipeline_id}).mappings().first()
            if p_result:
                parameters = p_result['parameters'] or {}
                source_type = p_result['source']

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
            if source_type and (source_type.startswith("http")):
                extracted = fetch_data_legacy(source_type, [])
                kwargs['ti'].xcom_push(key='extracted_data', value=extracted)
                return extracted
            raise Exception(f"No schema found for source {source_type}")

    connector_type = schema_result.get('connector_type')
    endpoint = schema_result.get('endpoint', 'default')
    base_url = schema_result.get('base_url')
    
    field_mappings = schema_result.get('field_mappings') or []

    print(f"[EXTRACT] Connector: {connector_type}, Endpoint: {endpoint}")

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
            if not parameters.get('extra_file_path'):
                 raise e
            print("[EXTRACT] Continuing with potential file data despite API error.")
    else:
        print(f"[EXTRACT] Legacy fetch: {source_type}")
        extracted = fetch_data_legacy(source_type, field_mappings)

    # 3. KIEGÉSZÍTŐ FÁJL KEZELÉSE (CASE-SENSITIVE MÓD)
    extra_file_path = parameters.get('extra_file_path')
    
    if extra_file_path and os.path.exists(extra_file_path):
        print(f"[MERGE] Processing extra file: {extra_file_path}")
        
        try:
            # A) API DataFrame
            api_df = pd.DataFrame(extracted)
            # ITT TÖRTÉNT A VÁLTOZÁS: Kivettük a .str.lower() hívást
            
            # B) File DataFrame
            ext = os.path.splitext(extra_file_path)[1].lower()
            file_df = pd.DataFrame()
            
            if ext == '.csv': file_df = pd.read_csv(extra_file_path)
            elif ext == '.json': file_df = pd.read_json(extra_file_path)
            elif ext == '.parquet': file_df = pd.read_parquet(extra_file_path)

            if not file_df.empty:
                # ITT IS: Kivettük a .str.lower() hívást - maradnak az eredeti nevek (pl. DIMONE)

                # --- DINAMIKUS JOIN LOGIKA ---
                api_cols = set(api_df.columns)
                file_cols = set(file_df.columns)
                
                # Csak a PONTOSAN egyező nevű oszlopokat tekinti közösnek
                common_cols = list(api_cols.intersection(file_cols))
                
                if common_cols:
                    print(f"[MERGE] Common columns found (Exact match): {common_cols}. Performing Outer Join.")
                    
                    # Típusbiztosítás (string) a pontos illesztéshez
                    for col in common_cols:
                        api_df[col] = api_df[col].astype(str).str.strip()
                        file_df[col] = file_df[col].astype(str).str.strip()
                    
                    merged_df = pd.merge(api_df, file_df, on=common_cols, how='outer')
                
                else:
                    # Ha nincs pontos egyezés, Concat
                    print(f"[MERGE] No common columns (Exact match). Appending rows (Concat).")
                    merged_df = pd.concat([api_df, file_df], ignore_index=True)

                # Output tisztítás: NaN -> None
                merged_df = merged_df.where(pd.notnull(merged_df), None)
                
                extracted = merged_df.to_dict(orient='records')
                print(f"[MERGE] Success. Final record count: {len(extracted)}")
            else:
                print("[MERGE] Warning: Extra file was empty.")

        except Exception as e:
            print(f"[MERGE ERROR] Failed to merge file: {e}")
            pass
            
    elif extra_file_path:
        print(f"[MERGE] Warning: File path found but file does not exist: {extra_file_path}")

    # 4. OUTPUT (NORMALIZÁLÁS NÉLKÜL)
    # A normalize_keys függvényt töröltük, hogy a kimenetben is megmaradjon a nagybetű (DIMONE)
    
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted)
    return extracted