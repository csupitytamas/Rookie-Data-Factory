"""
 Ez a modul a wizardban megadott mezőleképezési beállításokat értelmezi,
 meghatározva a végleges oszlopneveket és azok sorrendjét.
"""

# Az oszlop átnevezése
def resolve_final_column_name(column, field_mappings):
    mapping = field_mappings.get(column, {})

    # Ha az oszlop törlésre van jelölve, None értéket adunk vissza
    if mapping.get("delete", False):
        return None

    # Ha átnevezés van beállítva és van új név azt állítjuk be
    if mapping.get("rename", False) and mapping.get("newName"):
        return mapping["newName"]
    return column

# Végleges sorrend összeállítása a leképezések alapján
def get_final_column_order(column_order, field_mappings):
    result = []

    # Végigmegyünk a kért sorrenden és feloldjuk a véglegített oszlop neveket
    for col in column_order:
        final_name = resolve_final_column_name(col, field_mappings)
        if final_name:
            result.append(final_name)
    return result

# Az oszlop sorrend meghatározása
def get_all_final_columns(column_order, field_mappings):
    order_final = get_final_column_order(column_order, field_mappings)
    return order_final

# Segédfüggvény a mezőleképezések listájának összeállításához
def field_mapping_helper(field_mappings, column_order=None):
    columns = []

    # Aktív mezők összegyűjtése és esetleges átnevezése
    for col, props in field_mappings.items():
        if props.get("delete", False):
            continue
        final_name = props.get("newName") if props.get("rename", False) and props.get("newName") else col
        columns.append(final_name)
    columns = list(dict.fromkeys(columns))

    # Sorrendbe rendezés
    if column_order:
        final_order = [resolve_final_column_name(c, field_mappings) for c in column_order]
        columns = [col for col in final_order if col in columns]
    return columns
