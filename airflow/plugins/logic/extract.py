import sqlalchemy as sa
import os
import pandas as pd
from connector_helper import fetch_data_with_connector, fetch_data_legacy

def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése. 
    Módosítva: Támogatja a "Pipeline Dependency" módot (másik táblából olvasás).
    """
    print(f"[EXTRACT] Starting for Pipeline ID: {pipeline_id}")
    
    # Paraméterek kinyerése
    parameters = kwargs.get('parameters', {})
    source_type = kwargs.get('source_type')
    dependency_id = kwargs.get('dependency_id') # <--- Új paraméter a DAG-ból

    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    extracted = []

    # ---------------------------------------------------------
    # 1. ÁG: ADATOK BETÖLTÉSE MÁSIK PIPELINE-BÓL (DEPENDENCY)
    # ---------------------------------------------------------
    if dependency_id:
        print(f"[EXTRACT] 🔗 Dependency detected via Pipeline ID: {dependency_id}")
        
        try:
            with engine.connect() as conn:
                # A szülő pipeline kimeneti táblájának megkeresése
                # Fontos: int-re castoljuk, mert néha stringként jön a kwargs-ból
                parent_query = sa.text("SELECT target_table_name FROM etlconfig WHERE id = :pid")
                parent_result = conn.execute(parent_query, {"pid": int(dependency_id)}).mappings().first()
                
                if not parent_result or not parent_result['target_table_name']:
                    raise ValueError(f"Parent pipeline (ID: {dependency_id}) not found or has no target table!")
                
                target_table = parent_result['target_table_name']
                print(f"[EXTRACT] Reading from parent table: {target_table}")

                # Teljes tábla beolvasása Pandas DataFrame-be
                # A 'chunksize' opciót használhatod később, ha túl nagy a tábla
                df_parent = pd.read_sql(f"SELECT * FROM {target_table}", conn)
                
                if df_parent.empty:
                    print(f"[EXTRACT] ⚠️ Warning: Parent table '{target_table}' is empty!")
                
                # Átalakítás list of dict formátumra (hogy kompatibilis legyen a rendszer többi részével)
                # Dátum mezők stringgé alakítása javasolt a JSON szerializálás miatt
                extracted = df_parent.to_dict(orient='records')
                
                print(f"[EXTRACT] Successfully loaded {len(extracted)} rows from parent pipeline.")

        except Exception as e:
            print(f"[EXTRACT ERROR] Failed to load dependency: {e}")
            raise e # Ez kritikus hiba, ha a fő forrás nem elérhető

    # ---------------------------------------------------------
    # 2. ÁG: HAGYOMÁNYOS MŰKÖDÉS (API / FILE / LEGACY)
    # ---------------------------------------------------------
    else:
        # Ez a te EREDETI kódod, csak egy 'else' blokkba került
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

            # Ha nincs séma, de van source_type, próbálkozunk legacy móddal
            if not schema_result:
                print(f"[EXTRACT] ⚠️ No API Schema found for source '{source_type}'. Trying legacy mode.")
                if source_type and (source_type.startswith("http")):
                    extracted = fetch_data_legacy(source_type, [])
                    kwargs['ti'].xcom_push(key='extracted_data', value=extracted)
                    return extracted
                # Ha nem http és nem dependency, akkor itt megállhatunk (kivéve ha csak fájl)
                if not parameters.get('extra_file_path'):
                     raise Exception(f"No schema found for source {source_type}")
                connector_type = None
            else:
                connector_type = schema_result.get('connector_type')
                endpoint = schema_result.get('endpoint', 'default')
                base_url = schema_result.get('base_url')
                field_mappings = schema_result.get('field_mappings') or []

        # API Lekérés
        if connector_type:
            print(f"[EXTRACT] Connector: {connector_type}, Endpoint: {endpoint}")
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
        elif source_type and not dependency_id:
             # Legacy fetch fallback
             print(f"[EXTRACT] Legacy fetch: {source_type}")
             extracted = fetch_data_legacy(source_type, field_mappings)


    # ---------------------------------------------------------
    # 3. KIEGÉSZÍTŐ FÁJL KEZELÉSE (KÖZÖS RÉSZ)
    # Ez minden ágon lefut: Hozzáfűzi/Joinolja a fájlt, ha van.
    # ---------------------------------------------------------
    extra_file_path = parameters.get('extra_file_path')
    
    if extra_file_path and os.path.exists(extra_file_path):
        print(f"[MERGE] Processing extra file: {extra_file_path}")
        
        try:
            # A) Fő adatforrás (API vagy Pipeline DB)
            main_df = pd.DataFrame(extracted)
            
            # B) File DataFrame
            ext = os.path.splitext(extra_file_path)[1].lower()
            file_df = pd.DataFrame()
            
            if ext == '.csv': file_df = pd.read_csv(extra_file_path)
            elif ext == '.json': file_df = pd.read_json(extra_file_path)
            elif ext == '.parquet': file_df = pd.read_parquet(extra_file_path)

            if not file_df.empty:
                # --- DINAMIKUS JOIN LOGIKA ---
                main_cols = set(main_df.columns)
                file_cols = set(file_df.columns)
                
                # Csak a PONTOSAN egyező nevű oszlopokat tekinti közösnek
                common_cols = list(main_cols.intersection(file_cols))
                
                if common_cols:
                    print(f"[MERGE] Common columns found: {common_cols}. Performing Outer Join.")
                    
                    # Típusbiztosítás a joinhoz
                    for col in common_cols:
                        main_df[col] = main_df[col].astype(str).str.strip()
                        file_df[col] = file_df[col].astype(str).str.strip()
                    
                    merged_df = pd.merge(main_df, file_df, on=common_cols, how='outer')
                else:
                    print(f"[MERGE] No common columns. Appending rows (Concat).")
                    merged_df = pd.concat([main_df, file_df], ignore_index=True)

                # Output tisztítás: NaN -> None (JSON kompatibilitás miatt)
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

    # 4. OUTPUT
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted)
    return extracted