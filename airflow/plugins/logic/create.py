import sqlalchemy as sa
import json
import os
import re
from logic.type_converter import map_to_postgresql_type, guess_type
from logic.transforms.field_mapping import get_final_column_order, get_all_final_columns

""" 
CREATE TASK
A modul a dinamikus táblalétrehozásért felelős az ETL folyamat során. 
Meghatározza a PostgreSQL típusokat és létrehozza a cél-táblát a konfiguráció alapján.
"""

# Az oszlopnevek megtisztítása: minden nem-alfanumerikus karaktert alulvonásra cserél
def sanitize_column_name(col_name):
    if not isinstance(col_name, str):
        return str(col_name)
    name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if not name:
        name = "unnamed_column"
    return name

# Új tábla létrehozása
def create_table(pipeline_id, **kwargs):
    db_url = os.getenv("DB_URL") or os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not db_url:
        raise ValueError("Error: Database connection failed.")
    engine = sa.create_engine(db_url)

    # A konfiguráció lekérése az adatbázisból
    with engine.connect() as conn:
        query = sa.text("""
                        SELECT target_table_name,
                               field_mappings,
                               column_order,
                               update_mode,
                               group_by_columns,
                               order_by_column,
                               order_direction
                        FROM etlconfig
                        WHERE id = :id
                        """)

        # A pipeline konfigurációs adatainak kinyerése
        result = conn.execute(query, {"id": pipeline_id}).mappings().first()
        table_name = result['target_table_name']
        field_mappings = result['field_mappings']
        column_order = result['column_order']
        update_mode = result['update_mode']
        group_by_columns = result.get('group_by_columns')
        order_by_column = result.get('order_by_column')
        order_direction = result.get('order_direction')

        # A JSON formátumban tárolt beállítások feloldása
        if isinstance(field_mappings, str):
            field_mappings = json.loads(field_mappings)
        if isinstance(column_order, str):
            column_order = json.loads(column_order)
        if isinstance(group_by_columns, str):
            group_by_columns = json.loads(group_by_columns) if group_by_columns else []

        # A field_mappings listátját átalakítjuk dict-re a könnyebb kezelhetőség érdekében
        if isinstance(field_mappings, list):
            field_mappings_dict = {}
            for field in field_mappings:
                field_name = field.get('name')
                if field_name:
                    field_props = {k: v for k, v in field.items() if k != 'name'}
                    field_mappings_dict[field_name] = field_props
            field_mappings = field_mappings_dict

        # Ha nincs megadva mezőleképezés, automatikusan megpróbáljuk kitalálni az extract adatok alapján
        if not field_mappings:
            ti = kwargs["ti"]
            sample_rows = (
                    ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="extracted_data")
                    or ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}", key="return_value")
            )

            # Mintadatok ellenőrzése és formázása
            if isinstance(sample_rows, dict):
                sample_rows = [sample_rows]
            if not sample_rows:
                raise Exception("The source has no data!")

            # Automatikus típusmeghatározás az első sor alapján
            first = sample_rows[0]
            auto_field_mappings = {}
            for key, value in first.items():
                auto_field_mappings[key] = {
                    "type": guess_type(key, value),
                    "delete": False,
                    "rename": False,
                    "newName": ""
                }
            field_mappings = auto_field_mappings

        # Speciális kezelés az 'id' mezőhöz, hogy elkerüljük az ütközést a belső SERIAL ID-val
        if "id" in field_mappings:
            props = field_mappings["id"]
            if not props.get("rename"):
                props["rename"] = True
                props["newName"] = "source_id"
            elif props.get("rename") and props.get("newName") == "id":
                props["newName"] = "source_id"
        if not column_order and field_mappings:
            column_order = list(field_mappings.keys())

        # A végleges oszlopsorrend meghatározása
        unique_cols = [col for col, props in field_mappings.items() if props.get("unique")] if field_mappings else []
        final_columns = get_all_final_columns(
            column_order=column_order,
            field_mappings=field_mappings
        )

        # Duplikált oszlopnevek kiszűrése
        seen = set()
        deduped_columns = []
        for col in final_columns:
            if col == 'id':
                continue
            if col not in seen:
                seen.add(col)
                deduped_columns.append(col)
        final_columns = deduped_columns

        # Átnevezési mapping összeállítása az átalakításhoz
        col_rename_map = {}
        for orig, props in field_mappings.items():
            if props.get("delete", False):
                continue
            if props.get("rename", False) and props.get("newName"):
                col_rename_map[orig] = props["newName"]
            else:
                col_rename_map[orig] = orig

        # Segédfüggvény a végleges oszlopnév feloldásához
        def resolve_final(col):
            mapping = field_mappings.get(col, {})
            if mapping.get("delete", False):
                return None
            if mapping.get("rename", False) and mapping.get("newName"):
                return mapping["newName"]
            return col

        # A csoportosítási és rendezési oszlopok neveinek frissítése a későbbi lépésekhez
        mapped_group_by_columns = [resolve_final(col) for col in group_by_columns] if group_by_columns else []
        mapped_group_by_columns = [col for col in mapped_group_by_columns if col]
        mapped_order_by_column = resolve_final(order_by_column) if order_by_column else None
        column_defs = []

        # SQL oszlopdefiníciók generálása a típusok leképezésével
        for col in final_columns:
            clean_col = sanitize_column_name(col)

            # Az oszlophoz tartozó konfigurációs tulajdonságok megkeresése
            props = next((p for k, p in field_mappings.items()
                          if (p.get("rename") and p.get("newName") == col) or (not p.get("rename") and k == col)), {})
            raw_type = props.get('type', 'string')
            col_type = map_to_postgresql_type(raw_type)
            col_def = f'"{clean_col}" {col_type}'
            if update_mode == "upsert" and props.get('unique'):
                col_def += " UNIQUE"
            column_defs.append(col_def)
        if not column_defs:
            raise Exception("No columns to create.")

        # A tábla létrehozó SQL parancs összeállítása
        column_sql = ",\n  ".join(column_defs)
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
          id SERIAL PRIMARY KEY{',' if column_sql else ''}
          {column_sql}
        );'''

    # A tábla tényleges létrehozása és a metaadatok továbbadása XCom-on keresztül
    with engine.begin() as b_conn:
        b_conn.execute(sa.text(create_sql))
    ti = kwargs['ti']
    ti.xcom_push(key='final_columns', value=final_columns)
    ti.xcom_push(key='col_rename_map', value=col_rename_map)
    ti.xcom_push(key='group_by_columns', value=mapped_group_by_columns)
    ti.xcom_push(key='order_by_column', value=mapped_order_by_column)
    ti.xcom_push(key='order_direction', value=order_direction)
    ti.xcom_push(key='unique_cols', value=unique_cols)
    ti.xcom_push(key='field_mappings', value=field_mappings)
