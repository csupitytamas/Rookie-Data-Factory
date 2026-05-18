import sqlalchemy as sa
import pandas as pd
import os
import json
import re
from logic.transforms.data_load import load_overwrite, load_append, load_upsert
from logic.transforms.exporter import export_data
import pandas as pd

""" 
LOAD TASK
A modul felelős az adatok céladatbázisba történő betöltéséért és az opcionális fájlexportálásért. 
"""

db_url = os.getenv("DB_URL")
if not db_url:
    raise ValueError("Error: Database connection failed.")
engine = sa.create_engine(db_url)

# Új oszlopnevek megtisztítása: minden nem-alfanumerikus karaktert alulvonásra cserél
def sanitize_column_name(col_name):
    if not isinstance(col_name, str):
        return str(col_name)
    name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if not name:
        name = "unnamed_column"
    return name

# Load feladat végrehajtása
def load_data(pipeline_id, **kwargs):

    # A transzformációs lépés által előállított adatok lekérése XCom-ból.
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='final_data', task_ids=f"transform_data_{pipeline_id}")
    if not raw_data:
        print("No data to load!")
        raise Exception("No data to load!")
    data = []
    renamed_map = {}
    first_row_keys = list(raw_data[0].keys())

    # Oszlopnevek ellenőrzése és átnevezése, ha szükséges
    for key in first_row_keys:
        clean_key = sanitize_column_name(key)
        if key != clean_key:
            renamed_map[key] = clean_key
    if renamed_map:
        print(f"Renaming:")
        for old, new in renamed_map.items():
            print(f"   - '{old}'  ->  '{new}'")

    # Sorok feldolgozása: tisztítás és összetett típusok JSON formátumba alakítása.
    for row in raw_data:
        new_row = {}
        for k, v in row.items():
            clean_k = sanitize_column_name(k)
            if isinstance(v, (dict, list)):
                try:
                    new_row[clean_k] = json.dumps(v)
                except:
                    new_row[clean_k] = str(v)
            else:
                new_row[clean_k] = v
        data.append(new_row)

    # Pipeline konfigurációs adatok lekérdezése az etlconfig táblából.
    with engine.connect() as conn:
        query = sa.text("SELECT target_table_name, update_mode, file_format, save_option, custom_sql "
                        "FROM etlconfig "
                        "WHERE id = :id")
        result = conn.execute(query, {"id": pipeline_id}).mappings().first()
        table_name = result['target_table_name']
        update_mode = result['update_mode']
        file_format = result['file_format']
        save_option = result['save_option']
        custom_sql = result.get('custom_sql')

        # Az egyedi SQL használata esetén a séma ellenőrzése
        if custom_sql:
            data_columns = list(data[0].keys())
            inspector = sa.inspect(engine)

            # Létező tábla szerkezetének vizsgálata.
            if inspector.has_table(table_name):
                existing_columns = [col['name'] for col in inspector.get_columns(table_name)]

                # Ha a bejövő adatok sémája eltér a meglévőtől, a tábla cseréje.
                if set(data_columns) != set(existing_columns):
                    print(f"Schema mismatch! Required cols: {data_columns}, Existing cols: {existing_columns}")
                    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{table_name}"'))
                    df_struct = pd.DataFrame(data).head(0)
                    df_struct.to_sql(table_name, engine, if_exists='replace', index=False)

    # Fájlba történő mentés kezelés
    if save_option == "createfile":
        custom_path = None

        # A mentési útvonal lekérdezése a rendszerbeállítások közül.
        try:
            with engine.connect() as conn:
                settings_res = conn.execute(sa.text("SELECT download_path FROM system_settings LIMIT 1")).mappings().first()
                if settings_res and settings_res['download_path']:
                    custom_path = settings_res['download_path']
                    print(f"File create at: {custom_path}")
        except Exception as e:
            print(f"Save path not found, error: {e}")
        df = pd.DataFrame(data)
        export_data(df, table_name, file_format, output_path=None)

    # Adatok tényleges betöltése az adatbázisba.
    with engine.connect() as conn:

        # Az egyedi oszlopok lekérése az upsert művelethez.
        unique_cols_raw = ti.xcom_pull(key='unique_cols', task_ids=f"create_table_{pipeline_id}")
        unique_cols = []
        if unique_cols_raw:
            for col in unique_cols_raw:
                clean = sanitize_column_name(col)
                if clean in data[0].keys():
                    unique_cols.append(clean)

        # A kiválasztott frissítési mód szerinti betöltés végrehajtása.
        if update_mode == "overwrite":
            load_overwrite(table_name, data, conn)
        elif update_mode == "append":
            load_append(table_name, data, conn)
        elif update_mode == "upsert":
            if not unique_cols:
                 print("Upsert keys missing. Falling back to append.")
                 load_append(table_name, data, conn)
            else:
                load_upsert(table_name, data, conn, unique_cols=unique_cols)
    print(f"Data loading is done!")
