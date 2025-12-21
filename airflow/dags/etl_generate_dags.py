from numpy.distutils.conv_template import unique_key
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy as sa
import json
import requests
import pandas as pd
from transforms.data_load import load_overwrite, load_append, load_upsert
from transforms.field_mapping import field_mapping_helper, get_final_selected_columns, get_final_column_order, \
    resolve_final_column_name, get_concat_columns, get_all_final_columns
from transforms.transfomations import field_mapping, group_by, order_by, flatten_grouped_data
from transforms.exporter import export_data
from connector_helper import fetch_data_with_connector, fetch_data_legacy
# PostgreSQL connection
DB_URL = "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL"
engine = sa.create_engine(DB_URL)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def guess_type(key, value):
    """
    WHO-specifikus típuskövetkeztetés.
    A WHO 'value' mezője kaotikus lehet (pl. '31.9/34.5'), ezért mindig string.
    """
    if key == "value":
        return "string"

    # Normál logika
    if value is None:
        return "string"

    if isinstance(value, (int, float)):
        return "float"

    try:
        float(value)
        return "float"
    except:
        return "string"

# Type mapping: Python/JSON types -> PostgreSQL types
def map_to_postgresql_type(python_type: str) -> str:
    """
    Python/JSON típusok konvertálása PostgreSQL típusokká.
    
    Args:
        python_type: Python/JSON típus (pl. "string", "float", "integer")
        
    Returns:
        PostgreSQL típus (pl. "TEXT", "NUMERIC", "INTEGER")
    """
    type_mapping = {
        "string": "TEXT",
        "str": "TEXT",
        "text": "TEXT",
        "varchar": "VARCHAR(255)",
        "integer": "INTEGER",
        "int": "INTEGER",
        "float": "NUMERIC",
        "double": "NUMERIC",
        "numeric": "NUMERIC",
        "number": "NUMERIC",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "json": "JSONB",
        "jsonb": "JSONB",
        "array": "JSONB",
        "list": "JSONB",
    }
    
    # Case-insensitive lookup
    python_type_lower = python_type.lower().strip() if python_type else "string"
    
    # Ha már PostgreSQL típus (nagybetűvel kezdődik), akkor visszaadjuk
    if python_type and python_type[0].isupper():
        return python_type
    
    # Keresés a mapping-ben
    return type_mapping.get(python_type_lower, "TEXT")

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
# Create target table based on ETL config
def create_table(pipeline_id, **kwargs):
    print(f"\n=== [CREATE_TABLE] pipeline_id: {pipeline_id} ===")
    with engine.connect() as conn:
        query = sa.text("""
                        SELECT target_table_name,
                               field_mappings,
                               selected_columns,
                               column_order,
                               update_mode,
                               group_by_columns,
                               order_by_column,
                               order_direction
                        FROM etlconfig
                        WHERE id = :id
                        """)
        result = conn.execute(query, {"id": pipeline_id}).mappings().first()
        print("[CREATE_TABLE] Lekérdezett konfig:", result)
        table_name = result['target_table_name']
        field_mappings = result['field_mappings']
        selected_columns = result['selected_columns']
        column_order = result['column_order']
        update_mode = result['update_mode']
        group_by_columns = result.get('group_by_columns')
        order_by_column = result.get('order_by_column')
        order_direction = result.get('order_direction')

        # JSON decode, ha szükséges
        print("[CREATE_TABLE] JSON decode előtt:", field_mappings, selected_columns, column_order)
        if isinstance(field_mappings, str):
            field_mappings = json.loads(field_mappings)
        if isinstance(selected_columns, str):
            selected_columns = json.loads(selected_columns)
        if isinstance(column_order, str):
            column_order = json.loads(column_order)
        if isinstance(group_by_columns, str):
            group_by_columns = json.loads(group_by_columns) if group_by_columns else []

        # Field mappings formátum konverzió: lista -> dict (ha szükséges)
        # Az api_schemas táblában lista formátumban van: [{"name": "country", "type": "string", ...}]
        # De a create_table dict formátumot vár: {"country": {"type": "string", ...}}
        if isinstance(field_mappings, list):
            print("[CREATE_TABLE] Converting field_mappings from list to dict format")
            field_mappings_dict = {}
            for field in field_mappings:
                field_name = field.get('name')
                if field_name:
                    # Másoljuk az összes mezőt, de a 'name' mezőt kihagyjuk
                    field_props = {k: v for k, v in field.items() if k != 'name'}
                    field_mappings_dict[field_name] = field_props
            field_mappings = field_mappings_dict
            print(f"[CREATE_TABLE] Converted field_mappings: {list(field_mappings.keys())}")

        # 🔥 DYNAMIC SCHEMA DISCOVERY – ha nincs egyetlen field_mapping sem
        # (pl. WHO dynamic mode)
        if not field_mappings:
            print("[CREATE_TABLE] field_mappings üres → dinamikus séma felfedezés XCom-ból")

            ti = kwargs["ti"]

            # Próbáljuk a legvalószínűbb XCom key-ket
            sample_rows = (
                    ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="extracted_data")
                    or ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="return_value")
            )

            # Ha dict, tegyük listába
            if isinstance(sample_rows, dict):
                sample_rows = [sample_rows]

            if not sample_rows:
                raise Exception("Nem tudok dinamikus mezőket generálni, mert az extract_data nem adott vissza mintát!")

            # Első rekordból mezőlista
            first = sample_rows[0]
            print("[CREATE_TABLE] Minta rekord (XCom):", first)

            # Automatikus field_mappings generálás
            auto_field_mappings = {}
            for key, value in first.items():
                auto_field_mappings[key] = {
                    "type": guess_type(key, value),
                    "split": False,
                    "concat": {
                        "with": "",
                        "enabled": False,
                        "separator": " "
                    },
                    "delete": False,
                    "rename": False,
                    "newName": "",
                    "separator": ""
                }
            field_mappings = auto_field_mappings
            print("[CREATE_TABLE] AUTO-GENERATED field_mappings kulcsok:", list(field_mappings.keys()))

            # 🔥 FIX DUPLICATE id: rename WHO id -> who_id
            if "id" in field_mappings:
                print("[CREATE_TABLE] WHO API contains 'id' field → renaming to who_id")

                # move WHO id to new key
                field_mappings["who_id"] = field_mappings.pop("id")

                # fix selected_columns (if later auto-generated or user-provided)
                selected_columns = ["who_id" if c == "id" else c for c in selected_columns]

                # fix column_order
                column_order = ["who_id" if c == "id" else c for c in column_order]

            field_mappings = auto_field_mappings
            print("[CREATE_TABLE] AUTO-GENERATED field_mappings kulcsok:", list(field_mappings.keys()))

            # Ha a user nem jelölt ki explicit selected_columns / column_order-t,
            # akkor legyen az összes oszlop
            if not selected_columns:
                selected_columns = list(field_mappings.keys())
                print("[CREATE_TABLE] selected_columns üres volt → beállítva minden mezőre:", selected_columns)

            if not column_order:
                column_order = list(field_mappings.keys())
                print("[CREATE_TABLE] column_order üres volt → beállítva minden mezőre:", column_order)

        print("[CREATE_TABLE] JSON decode után:", field_mappings, selected_columns, column_order)

        unique_cols = [col for col, props in field_mappings.items() if props.get("unique")] if field_mappings else []
        print(f"[CREATE_TABLE] Unique oszlopok: {unique_cols}")
        concat_cols = get_concat_columns(field_mappings)
        print(f"[CREATE_TABLE] Concat cols: {concat_cols}")
        final_selected_columns = get_final_selected_columns(selected_columns, field_mappings)
        print(f"[CREATE_TABLE] final_selected_columns: {final_selected_columns}")

        final_column_order = get_final_column_order(column_order, field_mappings)
        print(f"[CREATE_TABLE] final_column_order: {final_column_order}")

        print("create_table - selected_columns:", selected_columns)
        print("create_table - column_order:", column_order)
        print("create_table - field_mappings:", field_mappings)

        final_columns = get_all_final_columns(
            selected_columns=final_selected_columns,
            column_order=final_column_order,
            field_mappings=field_mappings
        )

        print(f"[CREATE_TABLE] FINAL COLUMNS: {final_columns}")

        # Átnevezési mapping: eredeti név -> végleges név
        col_rename_map = {}
        for orig, props in field_mappings.items():
            if props.get("delete", False):
                continue
            if props.get("rename", False) and props.get("newName"):
                col_rename_map[orig] = props["newName"]
            else:
                col_rename_map[orig] = orig
        print(f"[CREATE_TABLE] col_rename_map: {col_rename_map}")

        # group_by, order_by mapping végleges névre
        def resolve_final(col):
            mapping = field_mappings.get(col, {})
            if mapping.get("delete", False):
                return None
            if mapping.get("rename", False) and mapping.get("newName"):
                return mapping["newName"]
            return col

        mapped_group_by_columns = [resolve_final(col) for col in group_by_columns] if group_by_columns else []
        mapped_group_by_columns = [col for col in mapped_group_by_columns if col]
        mapped_order_by_column = resolve_final(order_by_column) if order_by_column else None

        print(f"[CREATE_TABLE] mapped_group_by_columns: {mapped_group_by_columns}")
        print(f"[CREATE_TABLE] mapped_order_by_column: {mapped_order_by_column}")

        # --- Tábla létrehozás ---
        column_defs = []
        for col in final_columns:
            props = next((p for k, p in field_mappings.items()
                          if (p.get("rename") and p.get("newName") == col) or k == col), {})

            # Típus meghatározása és PostgreSQL-re konvertálása
            raw_type = props.get('type', 'string')
            col_type = map_to_postgresql_type(raw_type)

            col_def = f'"{col}" {col_type}'

            # NULL constraint
            if props.get('nullable') is False:
                col_def += " NOT NULL"

            # UNIQUE constraint (upsert módban)
            if update_mode == "upsert" and props.get('unique'):
                col_def += " UNIQUE"

            column_defs.append(col_def)
            print(f"[CREATE_TABLE] Column '{col}': type={raw_type} -> {col_type}, unique={props.get('unique', False)}")

        print(f"[CREATE_TABLE] column_defs: {column_defs}")

        if not column_defs:
            print("[CREATE_TABLE] ERROR: Nincs egyetlen oszlop sem!")
            raise Exception("A tábla nem hozható létre, mert nincs egyetlen oszlop sem a field mapping alapján!")

        column_sql = ",\n  ".join(column_defs)
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
          id SERIAL PRIMARY KEY{',' if column_sql else ''}
          {column_sql}
        );
        '''
        print("[CREATE_TABLE] CREATE SQL:\n", create_sql)
        conn.execute(sa.text(create_sql))
        print(f"[CREATE_TABLE] Tábla létrehozva vagy már létezett: {table_name}")

        # XCom push – minden szerkezet infó, amit a transform/load taskoknak tudni kell!
        ti = kwargs['ti']
        print(
            "[CREATE_TABLE] XCom push: final_columns, col_rename_map, group_by_columns, order_by_column, order_direction, unique_cols")
        ti.xcom_push(key='final_columns', value=final_columns)
        ti.xcom_push(key='col_rename_map', value=col_rename_map)
        ti.xcom_push(key='group_by_columns', value=mapped_group_by_columns)
        ti.xcom_push(key='order_by_column', value=mapped_order_by_column)
        ti.xcom_push(key='order_direction', value=order_direction)
        ti.xcom_push(key='unique_cols', value=unique_cols)
        ti.xcom_push(key='field_mappings', value=field_mappings)


# Data extraction from API
def extract_data(pipeline_id, **kwargs):
    """
    Adatok kinyerése API-ból connector réteg használatával.
    Ha van connector_type, akkor a connector-öket használja,
    egyébként a régi módszert (backward compatibility).
    """
    print(f"\n=== [EXTRACT_DATA] pipeline_id: {pipeline_id} ===")

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

def transform_data(pipeline_id, **kwargs):
    print(f"\n=== [TRANSFORM_DATA] pipeline_id: {pipeline_id} ===")
    ti = kwargs['ti']
    data = ti.xcom_pull(key='extracted_data', task_ids=f"extract_data_{pipeline_id}")
    print(f"[TRANSFORM_DATA] Extracted data (sample): {data[:2]}")

    if not data:
        print("[TRANSFORM_DATA] ERROR: Nincs adat az extractból!")
        raise Exception("Nincs adat az extractból!")

    final_columns = ti.xcom_pull(key='final_columns', task_ids=f"create_table_{pipeline_id}")
    col_rename_map = ti.xcom_pull(key='col_rename_map', task_ids=f"create_table_{pipeline_id}")
    group_by_columns = ti.xcom_pull(key='group_by_columns', task_ids=f"create_table_{pipeline_id}")
    order_by_column = ti.xcom_pull(key='order_by_column', task_ids=f"create_table_{pipeline_id}")
    order_direction = ti.xcom_pull(key='order_direction', task_ids=f"create_table_{pipeline_id}")
    # ÚJ: field_mappings beolvasása
    field_mappings = ti.xcom_pull(key='field_mappings', task_ids=f"create_table_{pipeline_id}")

    print(f"[TRANSFORM_DATA] final_columns: {final_columns}")
    print(f"[TRANSFORM_DATA] col_rename_map: {col_rename_map}")
    print(f"[TRANSFORM_DATA] group_by_columns: {group_by_columns}")
    print(f"[TRANSFORM_DATA] order_by_column: {order_by_column}, order_direction: {order_direction}")

    if not final_columns or not col_rename_map:
        print("[TRANSFORM_DATA] ERROR: Hiányzik a végleges szerkezet vagy mapping!")
        raise Exception("Nincs végleges szerkezet vagy mapping! Ellenőrizd a create_table task XCom push-okat!")

    # 1. Field mapping (átnevezés, törlés, sorrend, CONCAT is!)
    transformed = field_mapping(
        data, col_rename_map, final_columns, field_mappings=field_mappings
    )
    print(f"[TRANSFORM_DATA] Transformed (mapped) data (sample): {transformed[:2]}")
    ti.xcom_push(key='transformed_data', value=transformed)

    # 2. Rendezés
    if order_by_column:
        print(f"[TRANSFORM_DATA] Ordering by: {order_by_column} ({order_direction})")
        ordered = order_by(
            transformed,
            [order_by_column],
            order_direction
        )
    else:
        print("[TRANSFORM_DATA] No ordering applied.")
        ordered = transformed
    print(f"[TRANSFORM_DATA] Ordered data (sample): {ordered[:2]}")
    ti.xcom_push(key='ordered_data', value=ordered)

    # 3. Csoportosítás (group by)
    if group_by_columns:
        print(f"[TRANSFORM_DATA] Grouping by: {group_by_columns}")
        grouped = group_by(ordered, group_by_columns)
        ti.xcom_push(key='group_by_data', value=grouped)
        flattened = flatten_grouped_data(grouped)
        print(f"[TRANSFORM_DATA] Flattened grouped data (sample): {flattened[:2]}")
        ti.xcom_push(key='final_data', value=flattened)
    else:
        print("[TRANSFORM_DATA] No group by applied, final data is ordered data.")
        ti.xcom_push(key='final_data', value=ordered)

# Data load to target table
def load_data(pipeline_id, **kwargs):
    print(f"\n=== [LOAD_DATA] pipeline_id: {pipeline_id} ===")
    ti = kwargs['ti']
    data = ti.xcom_pull(key='final_data', task_ids=f"transform_data_{pipeline_id}")
    print(f"[LOAD_DATA] final_data (sample): {data[:2]}")

    if not data:
        print("[LOAD_DATA] ERROR: No data to load!")
        raise Exception("No data to load!")

    final_columns = ti.xcom_pull(key='final_columns', task_ids=f"create_table_{pipeline_id}")
    print(f"[LOAD_DATA] final_columns: {final_columns}")
    if not final_columns:
        print("[LOAD_DATA] ERROR: Nincs final_columns XCom-ban! Ellenőrizd a create_table-t.")
        raise Exception("Nincs final_columns XCom-ban! Ellenőrizd a create_table-t.")

    allowed_columns = set(final_columns)
    filtered_data = [{k: v for k, v in row.items() if k in allowed_columns} for row in data]
    print(f"[LOAD_DATA] filtered_data (sample): {filtered_data[:2]}")

    with engine.connect() as conn:
        query = sa.text("SELECT target_table_name, update_mode, file_format, save_option FROM etlconfig WHERE id = :id")
        result = conn.execute(query, {"id": pipeline_id}).mappings().first()
        print(f"[LOAD_DATA] etlconfig row: {result}")
        table_name = result['target_table_name']
        update_mode = result['update_mode']
        file_format = result['file_format']
        save_option = result['save_option']
        unique_cols = ti.xcom_pull(key='unique_cols', task_ids=f"create_table_{pipeline_id}")
        print(f"[LOAD_DATA] unique_cols: {unique_cols}")

        if update_mode == "overwrite":
            print(f"[LOAD_DATA] Mode: overwrite. Table: {table_name}")
            load_overwrite(table_name, filtered_data, conn)
        elif update_mode == "append":
            print(f"[LOAD_DATA] Mode: append. Table: {table_name}")
            load_append(table_name, filtered_data, conn)
        elif update_mode == "upsert":
            print(f"[LOAD_DATA] Mode: upsert. Table: {table_name}")
            if not unique_cols:
                print("[LOAD_DATA] ERROR: Upsert módban nincs unique mező!")
                raise Exception("Upsert módban legalább egy oszlopnál kötelező a unique mező!")
            load_upsert(table_name, filtered_data, conn, unique_cols=unique_cols)
        else:
            print(f"[LOAD_DATA] ERROR: Ismeretlen update_mode: {update_mode}")

        if save_option == "createfile":
            print(f"[LOAD_DATA] Export file, format: {file_format}")
            df = pd.DataFrame(filtered_data)
            export_data(df, table_name, file_format)
            print("Fájl exportálva:", table_name, file_format)

    print(f"[LOAD_DATA] {len(filtered_data)} rekord került betöltésre a(z) {table_name} táblába ({update_mode} móddal).")

# Pipeline select and DAG generate
with engine.connect() as conn:
    query = sa.text("SELECT * FROM etlconfig")
    pipelines = conn.execute(query).mappings().all()

for pipeline in pipelines:
    dag_id = pipeline['dag_id']
    print(f"[ETL DAG CREATE] dag_id: {dag_id}")
    schedule = pipeline['schedule']
    custom_time = pipeline.get('custom_time')

   # Schedule interval based on pipeline configuration
    if schedule == "daily":
        schedule_interval = "@daily"
    elif schedule == "hourly":
        schedule_interval = "@hourly"
    elif schedule == "weekly":
        schedule_interval = "@weekly"
    elif schedule == "monthly":
        schedule_interval == "@monthly"
    elif schedule == "yearly":
        schedule_interval = "@yearly"
    elif schedule == "once":
        schedule_interval = "@once"
    elif schedule == "never":
        schedule_interval = None
    elif schedule == "custom" and custom_time:
        hour, minute = custom_time.split(':')
        schedule_interval = f"{int(minute)} {int(hour)} * * *"
    else:
        schedule_interval = None


    # DAG definition
    dag = DAG(
        dag_id=dag_id,
        description=f"DAG for {pipeline['pipeline_name']}",
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        is_paused_upon_creation=False
    )
    # Task definitions
    create_task = PythonOperator(
        task_id=f"create_table_{pipeline['id']}",
        python_callable=create_table,
        op_args=[pipeline['id']],
        dag=dag,
    )

    extract_task = PythonOperator(
        task_id=f"extract_data_{pipeline['id']}",
        python_callable=extract_data,
        op_args=[pipeline['id']],
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f"transform_data_{pipeline['id']}",
        python_callable=transform_data,
        op_args=[pipeline['id']],
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f"load_data_{pipeline['id']}",
        python_callable=load_data,
        op_args=[pipeline['id']],
        dag=dag,
    )

    # Task dependencies
    extract_task >>create_task >> transform_task >> load_task

    globals()[dag_id] = dag
