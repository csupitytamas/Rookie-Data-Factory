import sqlalchemy as sa

""" Load lépés segédfüggvényei """

# Új adatok hozzáadása a táblához
def load_append(table_name, data, conn):
    if not data:
        return
    for row in data:
        keys = ', '.join([f'"{k}"' for k in row.keys()])
        params = ', '.join([f':{k}' for k in row.keys()])
        sql = sa.text(f'INSERT INTO "{table_name}" ({keys}) VALUES ({params})')
        conn.execute(sql, row)

# Adatok frissítése vagy beszúrása ütközés esetén (UPSERT)
def load_upsert(table_name, data, conn, unique_cols):
    if not data:
        print("[WARNING] No data to upsert.")
        return

    # Az oszlopnevek kinyerése az adatokból
    columns = list(data[0].keys())

    # A frissítendő oszlopok meghatározása (kivéve az egyedi kulcsokat és az id-t)
    update_cols = [col for col in columns if col not in unique_cols and col != 'id']
    update_stmt_parts = [f'"{col}"=EXCLUDED."{col}"' for col in update_cols]
    update_stmt = ", ".join(update_stmt_parts)

    # Az SQL kérés mezőinek és paramétereinek előkészítése
    keys_str = ", ".join([f'"{col}"' for col in columns])
    params_str = ", ".join([f':{col}' for col in columns])
    unique_col_formatted = f'"{unique_cols[0]}"'

    # SQL lekérdezés összeállítása:
    if update_stmt:
        sql_query = sa.text(f"""
            INSERT INTO "{table_name}" ({keys_str})
            VALUES ({params_str})
            ON CONFLICT ({unique_col_formatted}) DO UPDATE 
            SET {update_stmt};
        """)

    # Ha nincs frissítendő oszlop, csak hagyja figyelmen kívül az esetleges conflictot
    else:
        sql_query = sa.text(f"""
            INSERT INTO "{table_name}" ({keys_str})
            VALUES ({params_str})
            ON CONFLICT ({unique_col_formatted}) DO NOTHING;
        """)

    for row in data:
        conn.execute(sql_query, row)

# A tábla tartalmának teljes felülírása
def load_overwrite(table_name, data, conn):
    conn.execute(sa.text(f'TRUNCATE TABLE "{table_name}";'))
    load_append(table_name, data, conn)
