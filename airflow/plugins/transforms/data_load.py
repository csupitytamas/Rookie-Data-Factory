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
        return

    # 1. Először definiáljuk a 'columns'-t!
    columns = list(data[0].keys())

    # 2. Most már használhatjuk a 'columns'-t az SQL generáláshoz
    # Fontos: Idézőjelek ("") az oszlopnevek körül a nagybetűs nevek (pl. CODE) miatt!
    
    # "oszlop"=EXCLUDED."oszlop" formátum generálása
    update_stmt_parts = [f'"{col}"=EXCLUDED."{col}"' for col in columns if col not in unique_cols]
    update_stmt = ", ".join(update_stmt_parts)
    
    # "oszlop", "oszlop2" formátum
    cols_formatted = ", ".join([f'"{col}"' for col in columns])
    
    # %(oszlop)s, %(oszlop2)s formátum
    values_formatted = ", ".join([f"%({col})s" for col in columns])
    
    # Az egyedi kulcsot is idézőjelbe tesszük
    unique_col_formatted = f'"{unique_cols[0]}"'

    sql = f"""
    INSERT INTO "{table_name}" ({cols_formatted})
    VALUES ({values_formatted})
    ON CONFLICT ({unique_col_formatted}) DO UPDATE 
    SET {update_stmt};
    """

    print(f"[DEBUG] Upsert SQL: {sql}") # Opcionális: debuggoláshoz
    
    conn.execute(sql, data)

def load_overwrite(table_name, data, conn):
    conn.execute(sa.text(f'TRUNCATE TABLE "{table_name}";'))
    load_append(table_name, data, conn)