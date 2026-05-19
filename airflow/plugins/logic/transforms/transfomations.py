import pandas as pd
import sqlalchemy as sa

""" A transform lépés segédfüggvényei. """

# A field mapping beállítások tényleges végrehajtása
def field_mapping(data, col_rename_map, final_columns, field_mappings=None):
    transformed = []
    for i, row in enumerate(data):
        # Alapvető átnevezések végrehajtása
        new_row = dict(row)
        for orig, renamed in col_rename_map.items():
            if orig != renamed:
                new_row[renamed] = row.get(orig, "")

        # Mező-specifikus szabályok alkalmazása
        for orig_col, props in field_mappings.items():
            if props.get("delete", False):
                continue
            final_col = props.get("newName") if props.get("rename", False) and props.get("newName") else orig_col
            if final_col in new_row:
                continue
            new_row[final_col] = row.get(orig_col)

        # A végleges, rendezett adatsor összeállítása
        ordered_row = {col: new_row.get(col) for col in final_columns}
        transformed.append(ordered_row)
    return transformed

# Csoportosítás
def group_by(data, group_by_columns):
    grouped = {}

    # Kulcsok generálása és az elemek csoportba rendezése
    for row in data:
        key = tuple(row[k] for k in group_by_columns)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(row)

    # A csoportosított adatszerkezet visszaadása
    group_by_columns_result = [
        {
            "group": dict(zip(group_by_columns, key)),
            "items": items
        }
        for key, items in grouped.items()
    ]
    return group_by_columns_result

# Csoportosított adatok visszaalakítása listává
def flatten_grouped_data(grouped_data):
    flattened = []

    # A csoportkulcsok és az egyes elemek összeállítása
    for group in grouped_data:
        group_keys = group['group']
        for item in group['items']:
            flat_row = {**group_keys, **item}
            flattened.append(flat_row)
    return flattened

# Rendezés
def order_by(data, order_by_column, order_direction):
    if not order_by_column:
        print("No order_by_columns provided, returning original data.")
        return data

    # Rendezési kulcs összeállítása
    def sort_key(x):
        return tuple((x.get(col) if x.get(col) is not None else "") for col in order_by_column)

    # Rendezési irány
    if order_direction == "asc":
        order_by_result = sorted(data, key=sort_key)
    elif order_direction == "desc":
        order_by_result = sorted(data, key=sort_key, reverse=True)
    return order_by_result

# Egyedi felhasználói SQL lekérdezés futtatása (CTE wrapper)
def cte_wrapper(user_sql, actual_source_table, engine):
    clean_user_sql = user_sql.strip().rstrip(';')
    wrapper_sql = f"""
    WITH source AS (
    SELECT * FROM "{actual_source_table}") 
    SELECT * FROM ({clean_user_sql}) 
    AS user_query_wrapper"""
    try:
        with engine.connect() as conn:
            return pd.read_sql(sa.text(wrapper_sql), conn)
    except Exception as e:
        raise Exception(f"Error with the SQL: {e}")
