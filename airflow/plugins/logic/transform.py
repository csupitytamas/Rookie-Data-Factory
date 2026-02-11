import sqlalchemy as sa
import pandas as pd
import os
from transforms.transfomations import field_mapping, group_by, order_by, flatten_grouped_data

def transform_data(pipeline_id, **kwargs):
    print(f"\n=== [TRANSFORM_DATA] pipeline_id: {pipeline_id} ===")
    ti = kwargs['ti']
    
    # 1. Adatok kinyerése (Extract) - Ez mindig kell
    data = ti.xcom_pull(key='extracted_data', task_ids=f"extract_data_{pipeline_id}")
    # Fallback: ha esetleg return_value-ban van
    if not data:
        data = ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}")
        
    if not data:
        print("[TRANSFORM_DATA] ERROR: Nincs adat az extractból!")
        raise Exception("Nincs adat az extractból!")
    
    # 2. Pipeline Konfiguráció betöltése DB-ből (Hogy lássuk a custom_sql-t)
    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    custom_sql = None
    with engine.connect() as conn:
        # Lekérjük a custom_sql-t és a transformation típust
        query = sa.text("SELECT custom_sql, transformation FROM etlconfig WHERE id = :id")
        row = conn.execute(query, {"id": pipeline_id}).mappings().first()
        if row:
            raw_sql = row.get('custom_sql')
            transform_meta = row.get('transformation')
            
            # Csak akkor vesszük figyelembe, ha tényleg 'advenced' módban vagyunk
            # (és a string nem üres)
            if transform_meta and transform_meta.get('type') in ['advenced', 'advanced'] and raw_sql:
                custom_sql = raw_sql

    # --- ÁG 1: Custom SQL Végrehajtás ---
    if custom_sql:
        print(f"[TRANSFORM_DATA] 🚀 Executing Custom SQL Mode:\n{custom_sql}")
        
        try:
            # Pandas DataFrame létrehozása
            df = pd.DataFrame(data)
            
            # Memória SQL motor (SQLite)
            sqlite_engine = sa.create_engine('sqlite:///:memory:')
            
            # Adatok betöltése 'input_data' táblába
            df.to_sql('input_data', sqlite_engine, index=False, if_exists='replace')
            
            # SQL futtatása
            result_df = pd.read_sql(custom_sql, sqlite_engine)
            
            # Eredmény konvertálása vissza list-of-dicts formátumba
            transformed_data = result_df.to_dict(orient='records')
            
            print(f"[TRANSFORM_DATA] SQL Result sample (first 2): {transformed_data[:2]}")
            print(f"[TRANSFORM_DATA] Rows count: {len(transformed_data)}")
            
            # Mivel SQL-t használtunk, a mapping/sort/group lépéseket kihagyjuk,
            # mert feltételezzük, hogy az SQL mindent elintézett.
            ti.xcom_push(key='final_data', value=transformed_data)
            return

        except Exception as e:
            print(f"[TRANSFORM_DATA] ❌ SQL Execution Failed: {e}")
            raise Exception(f"Custom SQL execution error: {e}")

    # --- ÁG 2: Hagyományos Transzformáció (Ha nincs SQL) ---
    print("[TRANSFORM_DATA] No Custom SQL (or source table), fetching from XCom.")

    final_columns = ti.xcom_pull(key='final_columns', task_ids=f"create_table_{pipeline_id}")
    col_rename_map = ti.xcom_pull(key='col_rename_map', task_ids=f"create_table_{pipeline_id}")
    group_by_columns = ti.xcom_pull(key='group_by_columns', task_ids=f"create_table_{pipeline_id}")
    order_by_column = ti.xcom_pull(key='order_by_column', task_ids=f"create_table_{pipeline_id}")
    order_direction = ti.xcom_pull(key='order_direction', task_ids=f"create_table_{pipeline_id}")
    field_mappings = ti.xcom_pull(key='field_mappings', task_ids=f"create_table_{pipeline_id}")

    if not final_columns:
         # Ha valamiért nincs create_table info (pl. kézi indításnál elveszett),
         # próbáljuk meg az eredeti adatokat továbbadni.
         print("[TRANSFORM_DATA] WARNING: Missing final_columns from XCom. Passing raw data.")
         ti.xcom_push(key='final_data', value=data)
         return

    # 1. Field mapping
    transformed = field_mapping(data, col_rename_map, final_columns, field_mappings=field_mappings)
    
    # 2. Rendezés
    if order_by_column:
        ordered = order_by(transformed, [order_by_column], order_direction)
    else:
        ordered = transformed

    # 3. Csoportosítás
    if group_by_columns:
        grouped = group_by(ordered, group_by_columns)
        flattened = flatten_grouped_data(grouped)
        ti.xcom_push(key='final_data', value=flattened)
    else:
        print("[TRANSFORM_DATA] No group by applied, final data is ordered data.")
        ti.xcom_push(key='final_data', value=ordered)