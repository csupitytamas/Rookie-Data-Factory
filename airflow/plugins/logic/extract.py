import sqlalchemy as sa
import os
import pandas as pd
from connector_helper import fetch_data_with_connector


def extract_data(pipeline_id, **kwargs):
    parameters = kwargs.get('parameters', {})
    source_type = kwargs.get('source_type')
    dependency_id = kwargs.get('dependency_id')
    db_url = os.getenv("DB_URL") or os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

    if not db_url:
        raise ValueError("Error: Database connection failed.")

    engine = sa.create_engine(db_url)
    extracted_data = []

    with engine.connect() as conn:
        if not parameters:
            p_query = sa.text("SELECT parameters, source FROM etlconfig WHERE id = :id")
            p_result = conn.execute(p_query, {"id": pipeline_id}).mappings().first()
            if p_result:
                parameters = p_result['parameters'] or {}
                source_type = p_result['source']

        schema_query = sa.text("""
                               SELECT s.source,
                                      s.field_mappings,
                                      s.connector_type,
                                      s.endpoint,
                                      s.base_url
                               FROM api_schemas s
                               WHERE s.source = :source
                               """)
        schema_result = conn.execute(schema_query, {"source": source_type}).mappings().first()

        connector_type = None
        if schema_result:
            connector_type = schema_result.get('connector_type')
            endpoint = schema_result.get('endpoint', 'default')
            base_url = schema_result.get('base_url')
            field_mappings = schema_result.get('field_mappings') or []
        else:
            if not dependency_id and not parameters.get('extra_file_path'):
                print(f"No API Schema found for source '{source_type}'.")

    if connector_type:
        try:
            extracted_data = fetch_data_with_connector(
                connector_type=connector_type,
                endpoint=endpoint,
                parameters=parameters,
                base_url=base_url,
                field_mappings=field_mappings
            )
        except Exception as e:
            if not dependency_id and not parameters.get('extra_file_path'):
                raise e

    if dependency_id:
        try:
            with engine.connect() as conn:
                parent_query = sa.text("SELECT target_table_name FROM etlconfig WHERE id = :pid")
                parent_result = conn.execute(parent_query, {"pid": int(dependency_id)}).mappings().first()

                if parent_result and parent_result['target_table_name']:
                    target_table = parent_result['target_table_name']
                    df_parent = pd.read_sql(f'SELECT * FROM "{target_table}"', conn)

                    if not df_parent.empty:
                        df_main = pd.DataFrame(extracted_data)
                        if not df_main.empty:
                            common_cols = list(set(df_main.columns).intersection(set(df_parent.columns)))
                            if common_cols:
                                for col in common_cols:
                                    df_main[col] = df_main[col].astype(str).str.strip()
                                    df_parent[col] = df_parent[col].astype(str).str.strip()
                                merged_df = pd.merge(df_main, df_parent, on=common_cols, how='outer')
                            else:
                                merged_df = pd.concat([df_main, df_parent], ignore_index=True)
                            merged_df = merged_df.where(pd.notnull(merged_df), None)
                            extracted_data = merged_df.to_dict(orient='records')
                        else:
                            extracted_data = df_parent.to_dict(orient='records')
        except Exception as e:
            raise e

    extra_file_path = parameters.get('extra_file_path')
    if extra_file_path and os.path.exists(extra_file_path):
        try:
            main_df = pd.DataFrame(extracted_data)
            ext = os.path.splitext(extra_file_path)[1].lower()
            file_df = pd.DataFrame()
            if ext == '.csv':
                file_df = pd.read_csv(extra_file_path)
            elif ext == '.json':
                file_df = pd.read_json(extra_file_path)
            elif ext == '.parquet':
                file_df = pd.read_parquet(extra_file_path)

            if not file_df.empty:
                common_cols = list(set(main_df.columns).intersection(set(file_df.columns)))
                if common_cols:
                    for col in common_cols:
                        if col in main_df.columns: main_df[col] = main_df[col].astype(str).str.strip()
                        if col in file_df.columns: file_df[col] = file_df[col].astype(str).str.strip()
                    merged_df = pd.merge(main_df, file_df, on=common_cols, how='outer')
                else:
                    merged_df = pd.concat([main_df, file_df], ignore_index=True)
                extracted_data = merged_df.where(pd.notnull(merged_df), None).to_dict(orient='records')
        except Exception:
            pass

    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)
    return extracted_data