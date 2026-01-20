def get_concat_columns(field_mappings):
    """
    Visszaadja az összes concat által létrehozott új oszlop nevét,
    figyelembe véve a rename-eket is.
    """
    concat_columns = []
    concat_done = set()

    for col, props in field_mappings.items():
        concat = props.get("concat", {})
        if concat.get("enabled", False):
            other_col = concat.get("with")
            pair = tuple(sorted([col, other_col])) if other_col else None
            if other_col and field_mappings.get(other_col, {}).get("concat", {}).get("enabled", False):
                if pair in concat_done:
                    continue
                concat_done.add(pair)

                # figyeljük a rename-t mindkét oldalon
                col_name = props.get("newName") or col
                other_name = field_mappings[other_col].get("newName") or other_col

                # fix sorrendben (vagy ábécé szerint ha determinisztikát akarsz)
                new_col_name = f"{col_name}_{other_name}"
                concat_columns.append(new_col_name)
    return concat_columns

def get_final_selected_columns(selected_columns, field_mappings):
    """
    Visszaadja a végleges oszlopneveket a selected_columns alapján,
    figyelembe véve a rename-t, delete-et ÉS concat-ot.
    Ha concat van, a tagokat kihagyja (rename-elt nevükkel együtt), csak a concat mező marad.
    """
    final_cols = []
    concat_pairs = set()
    concat_names = get_concat_columns(field_mappings)

    # Összegyűjtjük azokat az oszlopokat, amik részt vesznek concat-ban (a rename-elt nevükkel együtt!)
    for col, props in field_mappings.items():
        concat = props.get("concat", {})
        if concat.get("enabled", False):
            other = concat.get("with")
            if other and field_mappings.get(other, {}).get("concat", {}).get("enabled", False):
                # ide már a végleges (rename-elt) neveket tesszük!
                col_name = props.get("newName") or col
                other_name = field_mappings[other].get("newName") or other
                concat_pairs.add(col_name)
                concat_pairs.add(other_name)

    # Csak akkor adjuk hozzá, ha nem tagja concat-nak (rename-elt nevükkel is nézzük!)
    for col in selected_columns:
        mapping = field_mappings.get(col, {})
        final_name = mapping.get("newName") if mapping.get("rename", False) and mapping.get("newName") else col
        if final_name in concat_pairs:
            continue  # kihagyjuk, mert concat fogja helyettesíteni
        if mapping.get("delete", False):
            continue
        final_cols.append(final_name)

    # Hozzáadjuk a concat mezőket
    for concat_col in concat_names:
        if concat_col not in final_cols:
            final_cols.append(concat_col)
    return final_cols

def get_final_column_order(column_order, field_mappings):
    """
    Átfordítja a column_order nevű listát végleges oszlopnevekre (átnevezés, törlés figyelembevételével),
    a concat tagokat (rename-elt nevükkel is!) kihagyja, és a concat mezőket a lista végére helyezi.
    """
    result = []
    concat_pairs = set()
    concat_names = get_concat_columns(field_mappings)

    # Összegyűjtjük a concat párok tagjait a rename-elt nevükkel együtt!
    for col, props in field_mappings.items():
        concat = props.get("concat", {})
        if concat.get("enabled", False):
            other = concat.get("with")
            if other and field_mappings.get(other, {}).get("concat", {}).get("enabled", False):
                col_name = props.get("newName") or col
                other_name = field_mappings[other].get("newName") or other
                concat_pairs.add(col_name)
                concat_pairs.add(other_name)

    # Csak akkor tesszük bele, ha nem concat tag (a végleges nevükkel is kizárjuk)
    for col in column_order:
        mapping = field_mappings.get(col, {})
        final_name = mapping.get("newName") if mapping.get("rename", False) and mapping.get("newName") else col
        if final_name in concat_pairs:
            continue  # kihagyjuk, mert concat váltja fel
        if mapping.get("delete", False):
            continue
        result.append(final_name)

    # A concat mezőket a végére tesszük, ha még nincsenek benne
    for concat_col in concat_names:
        if concat_col not in result:
            result.append(concat_col)

    return result

def resolve_final_column_name(column, field_mappings):
    mapping = field_mappings.get(column, {})
    if mapping.get("delete", False):
        return None
    if mapping.get("rename", False) and mapping.get("newName"):
        return mapping["newName"]
    return column

def get_all_final_columns(selected_columns, column_order, field_mappings):
    """
    Visszaadja a végleges oszlopneveket (rename, delete, concat figyelembevételével).
    Segédfüggvényként használja: get_final_selected_columns, get_final_column_order, get_concat_columns.
    """
    # 1. Feldolgozzuk a rename/delete-t a kiválasztottakon
    selected_final = get_final_selected_columns(selected_columns, field_mappings)
    order_final = get_final_column_order(column_order, field_mappings)
    # 2. Lekérjük a concat mezőneveket
    concat_names = get_concat_columns(field_mappings)
    # 3. A teljes mezőlista: sorrend szerint, concat is hozzáadva
    columns = [col for col in order_final if col in selected_final]
    # Hozzáadjuk a concat mezőket, ha nincsenek még benne
    for concat_col in concat_names:
        if concat_col not in columns:
            columns.append(concat_col)
    return columns

def field_mapping_helper(field_mappings, selected_columns=None, column_order=None):
    columns = []
    concat_done = set()
    concat_names = set()
    for col, props in field_mappings.items():
        if props.get("delete", False):
            continue

        concat = props.get("concat", {})
        if concat.get("enabled", False):
            other_col = concat.get("with")
            pair = tuple(sorted([col, other_col])) if other_col else None
            if other_col and field_mappings.get(other_col, {}).get("concat", {}).get("enabled", False):
                if pair not in concat_done:
                    # Új mezőnév: newName, vagy name_capital (abc szerint)
                    final_col = props.get("newName") or field_mappings[other_col].get("newName")
                    if not final_col:
                        final_col = "_".join(sorted([col, other_col]))
                    columns.append(final_col)
                    concat_names.add(final_col)
                    concat_done.add(pair)
                continue
            else:
                final_col = props.get("newName") or col
                columns.append(final_col)
                continue

        if props.get("rename", False) and props.get("newName"):
            columns.append(props["newName"])
        else:
            columns.append(col)

    columns = list(dict.fromkeys(columns))

    # Ez volt a hibád: a concat neveket NE szűrd le!
    if selected_columns:
        columns = [col for col in columns if (col in selected_columns or col in concat_names)]
    if column_order:
        columns = [col for col in column_order if col in columns or col in concat_names]
    print("[DEBUG] field_mapping_helper vége, columns:", columns)
    return columns