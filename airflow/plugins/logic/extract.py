import sqlalchemy as sa
import os
import pandas as pd
from connector_helper import fetch_data_with_connector, fetch_data_legacy

def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése. 
    JAVÍTOTT LOGIKA:
    1. API adatok lekérése (Saját indikátor)
    2. Dependency adatok lekérése (Szülő pipeline) -> MERGE (Outer Join)
    3. Extra File adatok lekérése -> MERGE (Outer Join)
    """
    print(f"[EXTRACT] Starting for Pipeline ID: {pipeline_id}")
    
    # Paraméterek kinyerése
    parameters = kwargs.get('parameters', {})
    source_type = kwargs.get('source_type')
    dependency_id = kwargs.get('dependency_id') 

    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    extracted_data = [] # Ez lesz a fő adathalmazunk

    # =========================================================
    # 1. LÉPÉS: SAJÁT ADATOK LEKÉRÉSE (API / LEGACY)
    # =========================================================
    # Ez most már mindig lefut, nem csak akkor, ha nincs dependency!
    
    with engine.connect() as conn:
        # Ha a paraméterek hiányoznak, pótoljuk DB-ből
        if not parameters:
            print("[EXTRACT] Parameters missing from kwargs, querying DB...")
            p_query = sa.text("SELECT parameters, source FROM etlconfig WHERE id = :id")
            p_result = conn.execute(p_query, {"id": pipeline_id}).mappings().first()
            if p_result:
                parameters = p_result['parameters'] or {}
                source_type = p_result['source']

        # API Séma betöltése
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

        connector_type = None
        if schema_result:
            connector_type = schema_result.get('connector_type')
            endpoint = schema_result.get('endpoint', 'default')
            base_url = schema_result.get('base_url')
            field_mappings = schema_result.get('field_mappings') or []
        else:
            # Ha nincs séma, de HTTP forrás
            if source_type and (source_type.startswith("http")):
                 print(f"[EXTRACT] Legacy fetch: {source_type}")
                 extracted_data = fetch_data_legacy(source_type, [])
            elif not dependency_id and not parameters.get('extra_file_path'):
                 # Csak akkor dobunk hibát, ha tényleg semmilyen forrás nincs (se dep, se file)
                 print(f"[EXTRACT] ⚠️ No API Schema found for source '{source_type}'.")

    # API hívás végrehajtása (ha van connector)
    if connector_type:
        print(f"[EXTRACT] Connector: {connector_type}, Endpoint: {endpoint}")
        try:
            extracted_data = fetch_data_with_connector(
                connector_type=connector_type,
                endpoint=endpoint,
                parameters=parameters,
                base_url=base_url,
                field_mappings=field_mappings
            )
            print(f"[EXTRACT] API fetch success! Records: {len(extracted_data)}")
        except Exception as e:
            print(f"[EXTRACT] Connector Error: {e}")
            # Ha van dependency vagy fájl, nem állunk meg hiba esetén sem, csak logolunk
            if not dependency_id and not parameters.get('extra_file_path'):
                 raise e
            print("[EXTRACT] Continuing despite API error (hoping for Dependency/File data).")


    # =========================================================
    # 2. LÉPÉS: MERGE WITH DEPENDENCY (SZÜLŐ PIPELINE)
    # =========================================================
    if dependency_id:
        print(f"[EXTRACT] 🔗 Dependency detected via Pipeline ID: {dependency_id}")
        
        try:
            with engine.connect() as conn:
                parent_query = sa.text("SELECT target_table_name FROM etlconfig WHERE id = :pid")
                parent_result = conn.execute(parent_query, {"pid": int(dependency_id)}).mappings().first()
                
                if parent_result and parent_result['target_table_name']:
                    target_table = parent_result['target_table_name']
                    print(f"[EXTRACT] Reading from parent table: {target_table}")

                    # Adatok betöltése DataFrame-be
                    df_parent = pd.read_sql(f"SELECT * FROM {target_table}", conn)
                    
                    if not df_parent.empty:
                        # --- MERGE LOGIKA (OUTER JOIN) ---
                        df_main = pd.DataFrame(extracted_data)
                        
                        if not df_main.empty:
                            # Keressük a közös oszlopokat
                            common_cols = list(set(df_main.columns).intersection(set(df_parent.columns)))
                            
                            if common_cols:
                                print(f"[DEP MERGE] Common columns: {common_cols}. Performing Outer Join.")
                                # Típuskonverzió a biztos egyezéshez
                                for col in common_cols:
                                    df_main[col] = df_main[col].astype(str).str.strip()
                                    df_parent[col] = df_parent[col].astype(str).str.strip()
                                
                                merged_df = pd.merge(df_main, df_parent, on=common_cols, how='outer')
                            else:
                                print(f"[DEP MERGE] No common columns. Appending data (Concat).")
                                merged_df = pd.concat([df_main, df_parent], ignore_index=True)
                            
                            # Visszaalakítás
                            merged_df = merged_df.where(pd.notnull(merged_df), None)
                            extracted_data = merged_df.to_dict(orient='records')
                        else:
                            # Ha az API üres volt, akkor a szülő adat lesz a fő adat
                            print(f"[DEP MERGE] API result was empty. Using only parent data.")
                            extracted_data = df_parent.to_dict(orient='records')
                    else:
                         print(f"[EXTRACT] ⚠️ Parent table '{target_table}' is empty.")
                else:
                    print(f"[EXTRACT] ⚠️ Parent pipeline has no target table.")

        except Exception as e:
            print(f"[EXTRACT ERROR] Failed to load dependency: {e}")
            raise e


    # =========================================================
    # 3. LÉPÉS: MERGE WITH EXTRA FILE (KÖZÖS RÉSZ)
    # =========================================================
    extra_file_path = parameters.get('extra_file_path')
    
    if extra_file_path and os.path.exists(extra_file_path):
        print(f"[MERGE] Processing extra file: {extra_file_path}")
        
        try:
            main_df = pd.DataFrame(extracted_data)
            
            ext = os.path.splitext(extra_file_path)[1].lower()
            file_df = pd.DataFrame()
            
            if ext == '.csv': file_df = pd.read_csv(extra_file_path)
            elif ext == '.json': file_df = pd.read_json(extra_file_path)
            elif ext == '.parquet': file_df = pd.read_parquet(extra_file_path)

            if not file_df.empty:
                # --- DINAMIKUS JOIN LOGIKA ---
                main_cols = set(main_df.columns)
                file_cols = set(file_df.columns)
                common_cols = list(main_cols.intersection(file_cols))
                
                if common_cols:
                    print(f"[FILE MERGE] Common columns: {common_cols}. Performing Outer Join.")
                    for col in common_cols:
                        if col in main_df.columns: main_df[col] = main_df[col].astype(str).str.strip()
                        if col in file_df.columns: file_df[col] = file_df[col].astype(str).str.strip()
                    
                    merged_df = pd.merge(main_df, file_df, on=common_cols, how='outer')
                else:
                    print(f"[FILE MERGE] No common columns. Appending rows (Concat).")
                    merged_df = pd.concat([main_df, file_df], ignore_index=True)

                merged_df = merged_df.where(pd.notnull(merged_df), None)
                extracted_data = merged_df.to_dict(orient='records')
                print(f"[FILE MERGE] Success. Final record count: {len(extracted_data)}")

        except Exception as e:
            print(f"[MERGE ERROR] Failed to merge file: {e}")
            pass

    # 4. OUTPUT
    print(f"[EXTRACT] Final dataset size: {len(extracted_data)}")
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)
    return extracted_data