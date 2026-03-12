import pandas as pd
import sqlalchemy as sa

def field_mapping(data, col_rename_map, final_columns, field_mappings=None):
    transformed = []
    for i, row in enumerate(data):
        # 1. Eredeti sor kibővítve az átnevezett kulcsokkal is
        new_row = dict(row)
        for orig, renamed in col_rename_map.items():
            if orig != renamed:
                new_row[renamed] = row.get(orig, "")

        concat_done = set()
        for orig_col, props in field_mappings.items():
            concat = props.get("concat", {})
            if concat.get("enabled", False):
                other_col = concat.get("with")
                pair = tuple(sorted([orig_col, other_col])) if other_col else None
                if other_col and field_mappings.get(other_col, {}).get("concat", {}).get("enabled", False):
                    if pair in concat_done:
                        continue
                    concat_done.add(pair)
                    sep = concat.get("separator", " ")
                    col1 = props.get("newName") or orig_col
                    col2 = field_mappings[other_col].get("newName") or other_col
                    new_col_name = f"{col1}_{col2}"
                    value1 = new_row.get(col1, "")
                    value2 = new_row.get(col2, "")
                    new_row[new_col_name] = f"{value1}{sep}{value2}"
                    continue

            if props.get("delete", False):
                continue
            final_col = props.get("newName") if props.get("rename", False) and props.get("newName") else orig_col
            if final_col in new_row:
                continue
            new_row[final_col] = row.get(orig_col)

        ordered_row = {col: new_row.get(col) for col in final_columns}
        transformed.append(ordered_row)

    return transformed

def group_by(data, group_by_columns):
    grouped = {}
    for row in data:
        key = tuple(row[k] for k in group_by_columns)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(row)
    group_by_columns_result = [
        {
            "group": dict(zip(group_by_columns, key)),
            "items": items
        }
        for key, items in grouped.items()
    ]
    return group_by_columns_result

def flatten_grouped_data(grouped_data):
    flattened = []
    for group in grouped_data:
        group_keys = group['group']
        for item in group['items']:
            flat_row = {**group_keys, **item}
            flattened.append(flat_row)
    return flattened


def order_by(data, order_by_column, order_direction):
    if not order_by_column:
        print("No order_by_columns provided, returning original data.")
        return data

    def sort_key(x):
        return tuple((x.get(col) if x.get(col) is not None else "") for col in order_by_column)

    if order_direction == "asc":
        order_by_result = sorted(data, key=sort_key)
    elif order_direction == "desc":
        order_by_result = sorted(data, key=sort_key, reverse=True)
    return order_by_result

def run_secure_sql_wrapper(user_sql, actual_source_table, engine):
    clean_user_sql = user_sql.strip().rstrip(';')
    wrapper_sql = f"""
    WITH input_data AS (
    SELECT * FROM "{actual_source_table}") 
    SELECT * FROM ({clean_user_sql}) 
    AS user_query_wrapper"""
    print(f"Executing:\n{wrapper_sql}")
    try:
        with engine.connect() as conn:
            return pd.read_sql(sa.text(wrapper_sql), conn)
    except Exception as e:
        raise Exception(f"Error with the SQL: {e}")

