import sqlalchemy as sa
import pandas as pd
import os
import json
import re
from transforms.data_load import load_overwrite, load_append, load_upsert
from transforms.exporter import export_data

def sanitize_column_name(col_name):
    """
    Ez a függvény a 'Takarító'.
    Bármilyen 'csúnya' SQL kimenetet átalakít Python/DB barát formátummá.
    """
    if not isinstance(col_name, str):
        return str(col_name)
    
    # 1. Specifikus cserék (hogy szebb legyen)
    name = col_name.replace("count(*)", "count_all")
    
    # 2. Minden nem-alfanumerikus karakter cseréje aláhúzásra
    # Pl: "sum(price)" -> "sum_price_", "Order Date" -> "Order_Date"
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    
    # 3. Többszörös aláhúzások összevonása (pl. "sum__price_" -> "sum_price")
    name = re.sub(r'_+', '_', name)
    
    # 4. Szélek tisztítása
    name = name.strip('_')
    
    # Ha véletlenül üres lenne (pl. a neve "???"), adjunk neki generált nevet
    if not name:
        name = "unnamed_column"
        
    return name.lower() # Legyen kisbetűs, az a legbiztosabb Postgresben

def load_data(pipeline_id, **kwargs):
    print(f"\n=== [LOAD_DATA] pipeline_id: {pipeline_id} ===")
    ti = kwargs['ti']
    
    # 1. Nyers adat átvétele
    raw_data = ti.xcom_pull(key='final_data', task_ids=f"transform_data_{pipeline_id}")
    
    if not raw_data:
        print("[LOAD_DATA] ERROR: No data to load!")
        raise Exception("No data to load!")

    # 2. 🔥 ADAT TISZTÍTÁS (SANITIZATION) 🔥
    # Itt alakítjuk át a kulcsokat (pl. "count(*)" -> "count_all")
    # Mielőtt bármit csinálnánk az adatbázissal!
    
    data = []
    # Eltároljuk a változásokat, hogy szóljunk a logban
    renamed_map = {} 
    
    # Csak az első sor alapján döntjük el a sémát
    first_row_keys = list(raw_data[0].keys())
    
    for key in first_row_keys:
        clean_key = sanitize_column_name(key)
        if key != clean_key:
            renamed_map[key] = clean_key

    if renamed_map:
        print(f"[LOAD_DATA] 🧹 Cleaning column names for database compatibility:")
        for old, new in renamed_map.items():
            print(f"   - '{old}'  ->  '{new}'")

    # Adatok átforgatása az új kulcsokkal
    for row in raw_data:
        new_row = {}
        for k, v in row.items():
            # A kulcsot tisztítjuk, az értéket megtartjuk
            clean_k = sanitize_column_name(k)
            
            # Ha az érték összetett (dict/list), JSON-osítjuk
            if isinstance(v, (dict, list)):
                try:
                    new_row[clean_k] = json.dumps(v)
                except:
                    new_row[clean_k] = str(v)
            else:
                new_row[clean_k] = v
        data.append(new_row)

    print(f"[LOAD_DATA] Cleaned data sample: {data[:1]}")

    # 3. Config betöltése
    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    
    with engine.connect() as conn:
        query = sa.text("SELECT target_table_name, update_mode, file_format, save_option, custom_sql FROM etlconfig WHERE id = :id")
        result = conn.execute(query, {"id": pipeline_id}).mappings().first()
        
        table_name = result['target_table_name']
        update_mode = result['update_mode']
        file_format = result['file_format']
        save_option = result['save_option']
        custom_sql = result.get('custom_sql')

        # 4. SÉMA EVOLÚCIÓ (A tisztított nevekkel!)
        if custom_sql:
            print("[LOAD_DATA] Custom SQL detected -> Checking schema match...")
            
            data_columns = list(data[0].keys()) # Ezek már a tiszta nevek (pl. count_all)
            
            inspector = sa.inspect(engine)
            if inspector.has_table(table_name):
                existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
                
                # Összehasonlítás
                if set(data_columns) != set(existing_columns):
                    print(f"[LOAD_DATA] Schema mismatch!")
                    print(f"   Required cols: {data_columns}")
                    print(f"   Existing cols: {existing_columns}")
                    print(f"   -> Dropping and Recreating table '{table_name}'...")
                    
                    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{table_name}"'))
                    
                    # Új tábla létrehozása a tisztított adatok alapján
                    df_struct = pd.DataFrame(data).head(0) 
                    df_struct.to_sql(table_name, engine, if_exists='replace', index=False)
                    print("[LOAD_DATA] Table recreated successfully.")

    # 5. Betöltés (vagy Export)
    if save_option == "createfile":
        print(f"[LOAD_DATA] Export file, format: {file_format}")
        df = pd.DataFrame(data)
        export_data(df, table_name, file_format)
        return

    with engine.connect() as conn:
        # Unique oszlopok lekérése (Upserthez)
        unique_cols_raw = ti.xcom_pull(key='unique_cols', task_ids=f"create_table_{pipeline_id}")
        
        # A unique oszlopok neveit is át kell fordítani, ha a felhasználó olyat adott meg, amit átneveztünk
        unique_cols = []
        if unique_cols_raw:
            for col in unique_cols_raw:
                clean = sanitize_column_name(col)
                if clean in data[0].keys():
                    unique_cols.append(clean)
        
        print(f"[LOAD_DATA] Loading {len(data)} rows into '{table_name}'...")

        if update_mode == "overwrite":
            load_overwrite(table_name, data, conn)
        elif update_mode == "append":
            load_append(table_name, data, conn)
        elif update_mode == "upsert":
            if not unique_cols:
                 print("[LOAD_DATA] WARNING: Upsert keys missing (maybe renamed/removed by SQL). Falling back to APPEND.")
                 load_append(table_name, data, conn)
            else:
                load_upsert(table_name, data, conn, unique_cols=unique_cols)
        else:
            print(f"[LOAD_DATA] ERROR: Unknown update_mode: {update_mode}")

    print(f"[LOAD_DATA] Success!")