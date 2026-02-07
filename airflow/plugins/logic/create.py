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

        # JSON decode
        if isinstance(field_mappings, str): field_mappings = json.loads(field_mappings)
        if isinstance(selected_columns, str): selected_columns = json.loads(selected_columns)
        if isinstance(column_order, str): column_order = json.loads(column_order)
        if isinstance(group_by_columns, str): group_by_columns = json.loads(group_by_columns) if group_by_columns else []

        # Lista -> Dict konverzió
        if isinstance(field_mappings, list):
            field_mappings_dict = {}
            for field in field_mappings:
                field_name = field.get('name')
                if field_name:
                    field_props = {k: v for k, v in field.items() if k != 'name'}
                    field_mappings_dict[field_name] = field_props
            field_mappings = field_mappings_dict

        # Dinamikus séma felfedezés
        if not field_mappings:
            print("[CREATE_TABLE] field_mappings üres → dinamikus séma felfedezés")
            ti = kwargs["ti"]
            sample_rows = (
                    ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="extracted_data")
                    or ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="return_value")
            )
            if isinstance(sample_rows, dict): sample_rows = [sample_rows]
            
            if not sample_rows:
                raise Exception("Nem tudok dinamikus mezőket generálni, nincs adat!")

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
            if not selected_columns: selected_columns = list(field_mappings.keys())
            if not column_order: column_order = list(field_mappings.keys())

        # ==============================================================================
        # 🔥 MÓDOSÍTOTT LOGIKA: DELETE DUPLICATE ID
        # Nem nevezzük át source_id-re, hanem teljesen töröljük a listából.
        # Így nem jön létre felesleges üres oszlop.
        # ==============================================================================
        if "id" in field_mappings:
            print("[CREATE_TABLE] 🗑️ 'id' column found in source. DROPPING it to avoid PK conflict and empty columns.")
            
            # 1. Töröljük a mappingből
            del field_mappings["id"]
            
            # 2. Kivesszük a listákból (szűrés)
            if selected_columns:
                selected_columns = [c for c in selected_columns if c != "id"]
            if column_order:
                column_order = [c for c in column_order if c != "id"]
        # ==============================================================================

        unique_cols = [col for col, props in field_mappings.items() if props.get("unique")] if field_mappings else []
        final_selected_columns = get_final_selected_columns(selected_columns, field_mappings)
        final_column_order = get_final_column_order(column_order, field_mappings)

        final_columns = get_all_final_columns(
            selected_columns=final_selected_columns,
            column_order=final_column_order,
            field_mappings=field_mappings
        )

        # Átnevezési térkép
        col_rename_map = {}
        for orig, props in field_mappings.items():
            if props.get("delete", False): continue
            if props.get("rename", False) and props.get("newName"):
                col_rename_map[orig] = props["newName"]
            else:
                col_rename_map[orig] = orig
        
        # Mivel töröltük az 'id'-t, NEM kell manuálisan hozzáadni a rename map-hez semmit.
        # A Transform lépés egyszerűen figyelmen kívül hagyja majd a bejövő 'id' adatot.

        # Group By / Order By feloldása
        def resolve_final(col):
            mapping = field_mappings.get(col, {})
            if mapping.get("delete", False): return None
            if mapping.get("rename", False) and mapping.get("newName"): return mapping["newName"]
            return col

        mapped_group_by_columns = [resolve_final(col) for col in group_by_columns] if group_by_columns else []
        mapped_group_by_columns = [col for col in mapped_group_by_columns if col]
        mapped_order_by_column = resolve_final(order_by_column) if order_by_column else None

        # CREATE TABLE SQL Generálás
        column_defs = []
        for col in final_columns:
            props = next((p for k, p in field_mappings.items() if (p.get("rename") and p.get("newName") == col) or k == col), {})
            
            raw_type = props.get('type', 'string')
            col_type = map_to_postgresql_type(raw_type)
            col_def = f'"{col}" {col_type}'

            if props.get('nullable') is False: col_def += " NOT NULL"
            if update_mode == "upsert" and props.get('unique'): col_def += " UNIQUE"

            column_defs.append(col_def)

        if not column_defs:
            raise Exception("ERROR: Nincs egyetlen oszlop sem a tábla létrehozásához!")

        column_sql = ",\n  ".join(column_defs)
        
        # Itt generálódik a technikai ID. Mivel a forrás ID-t kitöröltük, nem lesz ütközés.
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
          id SERIAL PRIMARY KEY{',' if column_sql else ''}
          {column_sql}
        );
        '''
        print("[CREATE_TABLE] CREATE SQL:\n", create_sql)
        conn.execute(sa.text(create_sql))
        print(f"[CREATE_TABLE] Tábla kész: {table_name}")

        ti = kwargs['ti']
        ti.xcom_push(key='final_columns', value=final_columns)
        ti.xcom_push(key='col_rename_map', value=col_rename_map)
        ti.xcom_push(key='group_by_columns', value=mapped_group_by_columns)
        ti.xcom_push(key='order_by_column', value=mapped_order_by_column)
        ti.xcom_push(key='order_direction', value=order_direction)
        ti.xcom_push(key='unique_cols', value=unique_cols)
        ti.xcom_push(key='field_mappings', value=field_mappings)