import sqlalchemy as sa
import json
import os
from logic.type_converter import map_to_postgresql_type, guess_type
from transforms.field_mapping import get_concat_columns, get_final_selected_columns, get_final_column_order, get_all_final_columns

def create_table(pipeline_id, **kwargs):
    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
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