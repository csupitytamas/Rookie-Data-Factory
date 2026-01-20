import sqlalchemy as sa

def load_append(table_name, data, conn):
    for row in data:
        keys = ', '.join([f'"{k}"' for k in row.keys()])
        values = ', '.join([f':{k}' for k in row.keys()])
        sql = sa.text(f'INSERT INTO "{table_name}" ({keys}) VALUES ({values})')
        conn.execute(sql, row)

def load_upsert(table_name, data, conn, unique_cols):
    for row in data:
        keys = list(row.keys())
        key_list = ', '.join([f'"{k}"' for k in keys])
        value_list = ', '.join([f':{k}' for k in keys])
        update_str = ', '.join([f'{k}=EXCLUDED.{k}' for k in keys if k not in unique_cols])
        conflict_cols = ', '.join([f'"{col}"' for col in unique_cols])
        sql = sa.text(
            f'INSERT INTO "{table_name}" ({key_list})\n'
            f'VALUES ({value_list})\n'
            f'ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_str};'
        )
        conn.execute(sql, row)

def load_overwrite(table_name, data, conn):
    conn.execute(sa.text(f'TRUNCATE TABLE "{table_name}";'))
    load_append(table_name, data, conn)