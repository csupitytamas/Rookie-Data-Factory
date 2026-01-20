import sqlalchemy as sa
import pandas as pd
import os
from transforms.data_load import load_overwrite, load_append, load_upsert
from transforms.exporter import export_data
def load_data(pipeline_id, **kwargs):
    print(f"\n=== [LOAD_DATA] pipeline_id: {pipeline_id} ===")
    ti = kwargs['ti']
    data = ti.xcom_pull(key='final_data', task_ids=f"transform_data_{pipeline_id}")
    print(f"[LOAD_DATA] final_data (sample): {data[:2]}")

    if not data:
        print("[LOAD_DATA] ERROR: No data to load!")
        raise Exception("No data to load!")

    final_columns = ti.xcom_pull(key='final_columns', task_ids=f"create_table_{pipeline_id}")
    print(f"[LOAD_DATA] final_columns: {final_columns}")
    if not final_columns:
        print("[LOAD_DATA] ERROR: Nincs final_columns XCom-ban! Ellenőrizd a create_table-t.")
        raise Exception("Nincs final_columns XCom-ban! Ellenőrizd a create_table-t.")

    allowed_columns = set(final_columns)
    filtered_data = [{k: v for k, v in row.items() if k in allowed_columns} for row in data]
    print(f"[LOAD_DATA] filtered_data (sample): {filtered_data[:2]}")
    db_url = os.getenv("DB_URL", "postgresql+psycopg2://postgres:admin123@host.docker.internal:5433/ETL")
    engine = sa.create_engine(db_url)
    with engine.connect() as conn:
        query = sa.text("SELECT target_table_name, update_mode, file_format, save_option FROM etlconfig WHERE id = :id")
        result = conn.execute(query, {"id": pipeline_id}).mappings().first()
        print(f"[LOAD_DATA] etlconfig row: {result}")
        table_name = result['target_table_name']
        update_mode = result['update_mode']
        file_format = result['file_format']
        save_option = result['save_option']
        unique_cols = ti.xcom_pull(key='unique_cols', task_ids=f"create_table_{pipeline_id}")
        print(f"[LOAD_DATA] unique_cols: {unique_cols}")

        if update_mode == "overwrite":
            print(f"[LOAD_DATA] Mode: overwrite. Table: {table_name}")
            load_overwrite(table_name, filtered_data, conn)
        elif update_mode == "append":
            print(f"[LOAD_DATA] Mode: append. Table: {table_name}")
            load_append(table_name, filtered_data, conn)
        elif update_mode == "upsert":
            print(f"[LOAD_DATA] Mode: upsert. Table: {table_name}")
            if not unique_cols:
                print("[LOAD_DATA] ERROR: Upsert módban nincs unique mező!")
                raise Exception("Upsert módban legalább egy oszlopnál kötelező a unique mező!")
            load_upsert(table_name, filtered_data, conn, unique_cols=unique_cols)
        else:
            print(f"[LOAD_DATA] ERROR: Ismeretlen update_mode: {update_mode}")

        if save_option == "createfile":
            print(f"[LOAD_DATA] Export file, format: {file_format}")
            df = pd.DataFrame(filtered_data)
            export_data(df, table_name, file_format)
            print("Fájl exportálva:", table_name, file_format)

    print(f"[LOAD_DATA] {len(filtered_data)} rekord került betöltésre a(z) {table_name} táblába ({update_mode} móddal).")