import sqlalchemy as sa

def load_append(table_name, data, conn):
    for row in data:
        keys = ', '.join([f'"{k}"' for k in row.keys()])
        values = ', '.join([f':{k}' for k in row.keys()])
        sql = sa.text(f'INSERT INTO "{table_name}" ({keys}) VALUES ({values})')
        conn.execute(sql, row)

def load_upsert(table_name, data, conn, unique_cols):
    if not data:
        print("[WARNING] No data to upsert.")
    columns = list(data[0].keys())
    update_stmt_parts = [f'"{col}"=EXCLUDED."{col}"' for col in columns if col not in unique_cols]
    update_stmt = ", ".join(update_stmt_parts)
    cols_formatted = ", ".join([f'"{col}"' for col in columns])
    values_formatted = ", ".join([f"%({col})s" for col in columns])
    unique_col_formatted = f'"{unique_cols[0]}"'
    sql = f"""
    INSERT INTO "{table_name}" ({cols_formatted})
    VALUES ({values_formatted})
    ON CONFLICT ({unique_col_formatted}) DO UPDATE 
    SET {update_stmt};
    """
    conn.execute(sql, data)

def load_overwrite(table_name, data, conn):
    conn.execute(sa.text(f'TRUNCATE TABLE "{table_name}";'))
    load_append(table_name, data, conn)