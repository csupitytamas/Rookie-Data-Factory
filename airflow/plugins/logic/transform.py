import sqlalchemy as sa
import pandas as pd
import os
from transforms.transfomations import field_mapping, group_by, order_by, flatten_grouped_data, run_secure_sql_wrapper

db_url = os.getenv("DB_URL")
if not db_url:
    raise ValueError("Error: Database connection failed.")
engine = sa.create_engine(db_url)

def transform_data(pipeline_id, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='extracted_data', task_ids=f"extract_data_{pipeline_id}")
    if not data:
        data = ti.xcom_pull(task_ids=f"extract_data_{pipeline_id}")

    if not data:
        print(" Error: No data to extract.")
        raise Exception("Error: No data to extract.")

    #Custom SQL futattása
    custom_sql = None
    with engine.connect() as conn:
        query = sa.text("SELECT custom_sql, transformation FROM etlconfig WHERE id = :id")
        row = conn.execute(query, {"id": pipeline_id}).mappings().first()
        if row:
            raw_sql = row.get('custom_sql')
            transform_meta = row.get('transformation')
            if transform_meta and transform_meta.get('type') in ['advenced', 'advanced'] and raw_sql:
                custom_sql = raw_sql
    if custom_sql:
        try:
            # CTE Wrapper alkalmazása ideiglenes táblán
            df = pd.DataFrame(data)
            staging_table = f"staging_p{pipeline_id}"
            df.to_sql(staging_table, engine, if_exists='replace', index=False)
            result_df = run_secure_sql_wrapper(
                user_sql=custom_sql,
                actual_source_table=staging_table,
                engine=engine
            )
            # Ideiglenes tábla kiürítése
            with engine.connect() as conn:
                conn.execute(sa.text(f'DROP TABLE IF EXISTS "{staging_table}"'))
                conn.commit()

            # Mentés XCom-ba
            transformed_data = result_df.to_dict(orient='records')
            ti.xcom_push(key='final_data', value=transformed_data)
            return

        except Exception as e:
            print(f"Error with the Custom SQL {e}")
            raise

        except Exception as e:
            print(f" SQL Execution Failed: {e}")
            raise Exception(f"Custom SQL execution error: {e}")

    final_columns = ti.xcom_pull(key='final_columns', task_ids=f"create_table_{pipeline_id}")
    col_rename_map = ti.xcom_pull(key='col_rename_map', task_ids=f"create_table_{pipeline_id}")
    group_by_columns = ti.xcom_pull(key='group_by_columns', task_ids=f"create_table_{pipeline_id}")
    order_by_column = ti.xcom_pull(key='order_by_column', task_ids=f"create_table_{pipeline_id}")
    order_direction = ti.xcom_pull(key='order_direction', task_ids=f"create_table_{pipeline_id}")
    field_mappings = ti.xcom_pull(key='field_mappings', task_ids=f"create_table_{pipeline_id}")

    if not final_columns:
         print("WARNING: Missing final_columns from XCom. Passing raw data.")
         ti.xcom_push(key='final_data', value=data)
         return

    # 1. Field mapping
    transformed = field_mapping(data, col_rename_map, final_columns, field_mappings=field_mappings)
    # 2. Order_by
    if order_by_column:
        ordered = order_by(transformed, [order_by_column], order_direction)
    else:
        ordered = transformed
    # 3. Group_by
    if group_by_columns:
        grouped = group_by(ordered, group_by_columns)
        flattened = flatten_grouped_data(grouped)
        ti.xcom_push(key='final_data', value=flattened)
    else:
        ti.xcom_push(key='final_data', value=ordered)
    print("Transforming is ready!")