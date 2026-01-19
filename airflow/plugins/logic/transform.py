from transforms.transfomations import field_mapping, group_by, order_by, flatten_grouped_data

def transform_data(pipeline_id, **kwargs):
    print(f"\n=== [TRANSFORM_DATA] pipeline_id: {pipeline_id} ===")
    ti = kwargs['ti']
    data = ti.xcom_pull(key='extracted_data', task_ids=f"extract_data_{pipeline_id}")
    print(f"[TRANSFORM_DATA] Extracted data (sample): {data[:2]}")

    if not data:
        print("[TRANSFORM_DATA] ERROR: Nincs adat az extractból!")
        raise Exception("Nincs adat az extractból!")

    final_columns = ti.xcom_pull(key='final_columns', task_ids=f"create_table_{pipeline_id}")
    col_rename_map = ti.xcom_pull(key='col_rename_map', task_ids=f"create_table_{pipeline_id}")
    group_by_columns = ti.xcom_pull(key='group_by_columns', task_ids=f"create_table_{pipeline_id}")
    order_by_column = ti.xcom_pull(key='order_by_column', task_ids=f"create_table_{pipeline_id}")
    order_direction = ti.xcom_pull(key='order_direction', task_ids=f"create_table_{pipeline_id}")
    # ÚJ: field_mappings beolvasása
    field_mappings = ti.xcom_pull(key='field_mappings', task_ids=f"create_table_{pipeline_id}")

    print(f"[TRANSFORM_DATA] final_columns: {final_columns}")
    print(f"[TRANSFORM_DATA] col_rename_map: {col_rename_map}")
    print(f"[TRANSFORM_DATA] group_by_columns: {group_by_columns}")
    print(f"[TRANSFORM_DATA] order_by_column: {order_by_column}, order_direction: {order_direction}")

    if not final_columns or not col_rename_map:
        print("[TRANSFORM_DATA] ERROR: Hiányzik a végleges szerkezet vagy mapping!")
        raise Exception("Nincs végleges szerkezet vagy mapping! Ellenőrizd a create_table task XCom push-okat!")

    # 1. Field mapping (átnevezés, törlés, sorrend, CONCAT is!)
    transformed = field_mapping(
        data, col_rename_map, final_columns, field_mappings=field_mappings
    )
    print(f"[TRANSFORM_DATA] Transformed (mapped) data (sample): {transformed[:2]}")
    ti.xcom_push(key='transformed_data', value=transformed)

    # 2. Rendezés
    if order_by_column:
        print(f"[TRANSFORM_DATA] Ordering by: {order_by_column} ({order_direction})")
        ordered = order_by(
            transformed,
            [order_by_column],
            order_direction
        )
    else:
        print("[TRANSFORM_DATA] No ordering applied.")
        ordered = transformed
    print(f"[TRANSFORM_DATA] Ordered data (sample): {ordered[:2]}")
    ti.xcom_push(key='ordered_data', value=ordered)

    # 3. Csoportosítás (group by)
    if group_by_columns:
        print(f"[TRANSFORM_DATA] Grouping by: {group_by_columns}")
        grouped = group_by(ordered, group_by_columns)
        ti.xcom_push(key='group_by_data', value=grouped)
        flattened = flatten_grouped_data(grouped)
        print(f"[TRANSFORM_DATA] Flattened grouped data (sample): {flattened[:2]}")
        ti.xcom_push(key='final_data', value=flattened)
    else:
        print("[TRANSFORM_DATA] No group by applied, final data is ordered data.")
        ti.xcom_push(key='final_data', value=ordered)