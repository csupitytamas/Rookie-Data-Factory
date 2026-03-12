def resolve_final_column_name(column, field_mappings):
    mapping = field_mappings.get(column, {})
    if mapping.get("delete", False):
        return None
    if mapping.get("rename", False) and mapping.get("newName"):
        return mapping["newName"]
    return column

def get_final_selected_columns(selected_columns, field_mappings):
    final_cols = []
    for col in selected_columns:
        final_name = resolve_final_column_name(col, field_mappings)
        if final_name:
            final_cols.append(final_name)
    return final_cols

def get_final_column_order(column_order, field_mappings):
    result = []
    for col in column_order:
        final_name = resolve_final_column_name(col, field_mappings)
        if final_name:
            result.append(final_name)
    return result

def get_all_final_columns(selected_columns, column_order, field_mappings):
    selected_final = get_final_selected_columns(selected_columns, field_mappings)
    order_final = get_final_column_order(column_order, field_mappings)
    columns = [col for col in order_final if col in selected_final]
    return columns

def field_mapping_helper(field_mappings, selected_columns=None, column_order=None):
    columns = []
    for col, props in field_mappings.items():
        # Törölteket kihagyjuk
        if props.get("delete", False):
            continue
        # Átnevezés
        final_name = props.get("newName") if props.get("rename", False) and props.get("newName") else col
        columns.append(final_name)

    columns = list(dict.fromkeys(columns))

    if selected_columns:
        final_selected = [resolve_final_column_name(c, field_mappings) for c in selected_columns]
        columns = [col for col in columns if col in final_selected]

    if column_order:
        final_order = [resolve_final_column_name(c, field_mappings) for c in column_order]
        columns = [col for col in final_order if col in columns]

    return columns