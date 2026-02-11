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
        if isinstance(field_mappings, str):
            field_mappings = json.loads(field_mappings)
        if isinstance(selected_columns, str):
            selected_columns = json.loads(selected_columns)
        if isinstance(column_order, str):
            column_order = json.loads(column_order)
        if isinstance(group_by_columns, str):
            group_by_columns = json.loads(group_by_columns) if group_by_columns else []

        # Field mappings formátum konverzió: lista -> dict
        if isinstance(field_mappings, list):
            print("[CREATE_TABLE] Converting field_mappings from list to dict format")
            field_mappings_dict = {}
            for field in field_mappings:
                field_name = field.get('name')
                if field_name:
                    field_props = {k: v for k, v in field.items() if k != 'name'}
                    field_mappings_dict[field_name] = field_props
            field_mappings = field_mappings_dict

        # 🔥 DYNAMIC SCHEMA DISCOVERY – ha nincs egyetlen field_mapping sem
        if not field_mappings:
            print("[CREATE_TABLE] field_mappings üres → dinamikus séma felfedezés XCom-ból")
            ti = kwargs["ti"]
            sample_rows = (
                    ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="extracted_data")
                    or ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="return_value")
            )

            if isinstance(sample_rows, dict):
                sample_rows = [sample_rows]

            if not sample_rows:
                raise Exception("Nem tudok dinamikus mezőket generálni, mert az extract_data nem adott vissza mintát!")

            first = sample_rows[0]
            auto_field_mappings = {}
            for key, value in first.items():
                auto_field_mappings[key] = {
                    "type": guess_type(key, value),
                    "split": False,
                    "concat": {"with": "", "enabled": False, "separator": " "},
                    "delete": False,
                    "rename": False,
                    "newName": "",
                    "separator": ""
                }
            field_mappings = auto_field_mappings
            
            # FIX DUPLICATE id: rename WHO id -> who_id
            if "id" in field_mappings:
                field_mappings["who_id"] = field_mappings.pop("id")

        # --- JAVÍTÁS ITT ---
        # Ha a selected_columns üres (pl. Advanced SQL mód miatt), 
        # akkor alapértelmezésben vegyük az összes mezőt a field_mappings-ből.
        # Ez a blokk korábban csak a 'if not field_mappings' ágon belül volt,
        # most kihoztuk ide, hogy mindig fusson.
        if not selected_columns and field_mappings:
            print("[CREATE_TABLE] selected_columns üres volt (pl. Advanced Mode) → Minden mező kiválasztása.")
            selected_columns = list(field_mappings.keys())
            
        if not column_order and field_mappings:
            column_order = list(field_mappings.keys())
        # -------------------

        print("[CREATE_TABLE] Konfiguráció véglegesítve:", selected_columns)

        unique_cols = [col for col, props in field_mappings.items() if props.get("unique")] if field_mappings else []
        concat_cols = get_concat_columns(field_mappings)
        final_selected_columns = get_final_selected_columns(selected_columns, field_mappings)
        final_column_order = get_final_column_order(column_order, field_mappings)

        final_columns = get_all_final_columns(
            selected_columns=final_selected_columns,
            column_order=final_column_order,
            field_mappings=field_mappings
        )

        print(f"[CREATE_TABLE] FINAL COLUMNS: {final_columns}")

        # Átnevezési mapping
        col_rename_map = {}
        for orig, props in field_mappings.items():
            if props.get("delete", False):
                continue
            if props.get("rename", False) and props.get("newName"):
                col_rename_map[orig] = props["newName"]
            else:
                col_rename_map[orig] = orig

        # group_by, order_by feloldás
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

        # --- Tábla létrehozás ---
        column_defs = []
        for col in final_columns:
            props = next((p for k, p in field_mappings.items()
                          if (p.get("rename") and p.get("newName") == col) or k == col), {})

            raw_type = props.get('type', 'string')
            col_type = map_to_postgresql_type(raw_type)

            col_def = f'"{col}" {col_type}'

            if props.get('nullable') is False:
                col_def += " NOT NULL"

            if update_mode == "upsert" and props.get('unique'):
                col_def += " UNIQUE"

            column_defs.append(col_def)

        if not column_defs:
            print("[CREATE_TABLE] ERROR: Még mindig nincs oszlop!")
            raise Exception("A tábla nem hozható létre, mert nincs egyetlen oszlop sem!")

        column_sql = ",\n  ".join(column_defs)
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
          id SERIAL PRIMARY KEY{',' if column_sql else ''}
          {column_sql}
        );
        '''
        print("[CREATE_TABLE] CREATE SQL:\n", create_sql)
        conn.execute(sa.text(create_sql))
        print(f"[CREATE_TABLE] Tábla létrehozva: {table_name}")

        # XCom push
        ti = kwargs['ti']
        ti.xcom_push(key='final_columns', value=final_columns)
        ti.xcom_push(key='col_rename_map', value=col_rename_map)
        ti.xcom_push(key='group_by_columns', value=mapped_group_by_columns)
        ti.xcom_push(key='order_by_column', value=mapped_order_by_column)
        ti.xcom_push(key='order_direction', value=order_direction)
        ti.xcom_push(key='unique_cols', value=unique_cols)
        ti.xcom_push(key='field_mappings', value=field_mappings)