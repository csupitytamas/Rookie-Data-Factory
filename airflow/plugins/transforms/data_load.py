import sqlalchemy as sa

def load_append(table_name, data, conn):
    if not data:
        return
    for row in data:
        keys = ', '.join([f'"{k}"' for k in row.keys()])
        params = ', '.join([f':{k}' for k in row.keys()])
        sql = sa.text(f'INSERT INTO "{table_name}" ({keys}) VALUES ({params})')
        conn.execute(sql, row)

def load_upsert(table_name, data, conn, unique_cols):
    if not data:
        print("[WARNING] No data to upsert.")
        return

    # Szedjük ki az összes oszlopot az első sorból
    columns = list(data[0].keys())
    
    # Felkészítjük a SET részt (azokat az oszlopokat, amiket frissíteni kell ütközés esetén)
    # Kizárjuk az 'id'-t (ami autoincrement) és az egyedi oszlopokat
    update_cols = [col for col in columns if col not in unique_cols and col != 'id']
    update_stmt_parts = [f'"{col}"=EXCLUDED."{col}"' for col in update_cols]
    update_stmt = ", ".join(update_stmt_parts)
    
    # Felkészítjük a mezőket és a paramétereket az INSERT részhez
    keys_str = ", ".join([f'"{col}"' for col in columns])
    params_str = ", ".join([f':{col}' for col in columns])
    
    # Az első egyedi oszlopot használjuk az ütközés vizsgálathoz
    unique_col_formatted = f'"{unique_cols[0]}"'
    
    # Összerakjuk a végleges SQL-t
    if update_stmt:
        sql_query = sa.text(f"""
            INSERT INTO "{table_name}" ({keys_str})
            VALUES ({params_str})
            ON CONFLICT ({unique_col_formatted}) DO UPDATE 
            SET {update_stmt};
        """)
    else:
        # Ha nincs mit frissíteni (pl. csak egyedi oszlopunk van), akkor csak kihagyjuk az ütközést
        sql_query = sa.text(f"""
            INSERT INTO "{table_name}" ({keys_str})
            VALUES ({params_str})
            ON CONFLICT ({unique_col_formatted}) DO NOTHING;
        """)

    # Soronként hajtjuk végre a biztonságos paraméter átadás érdekében
    for row in data:
        conn.execute(sql_query, row)

def load_overwrite(table_name, data, conn):
    # Truncate után az append már biztonságosan használja az idézőjeleket
    conn.execute(sa.text(f'TRUNCATE TABLE "{table_name}";'))
    load_append(table_name, data, conn)
