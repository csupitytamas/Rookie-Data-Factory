def guess_type(key, value):
    """
    WHO-specifikus típuskövetkeztetés.
    A WHO 'value' mezője kaotikus lehet (pl. '31.9/34.5'), ezért mindig string.
    """
    if key == "value":
        return "string"

    # Normál logika
    if value is None:
        return "string"

    if isinstance(value, (int, float)):
        return "float"

    try:
        float(value)
        return "float"
    except:
        return "string"

def map_to_postgresql_type(python_type: str) -> str:
    """
    Python/JSON típusok konvertálása PostgreSQL típusokká.

    Args:
        python_type: Python/JSON típus (pl. "string", "float", "integer")

    Returns:
        PostgreSQL típus (pl. "TEXT", "NUMERIC", "INTEGER")
    """
    type_mapping = {
        "string": "TEXT",
        "str": "TEXT",
        "text": "TEXT",
        "varchar": "VARCHAR(255)",
        "integer": "INTEGER",
        "int": "INTEGER",
        "float": "NUMERIC",
        "double": "NUMERIC",
        "numeric": "NUMERIC",
        "number": "NUMERIC",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "json": "JSONB",
        "jsonb": "JSONB",
        "array": "JSONB",
        "list": "JSONB",
    }

    # Case-insensitive lookup
    python_type_lower = python_type.lower().strip() if python_type else "string"

    # Ha már PostgreSQL típus (nagybetűvel kezdődik), akkor visszaadjuk
    if python_type and python_type[0].isupper():
        return python_type

    # Keresés a mapping-ben
    return type_mapping.get(python_type_lower, "TEXT")