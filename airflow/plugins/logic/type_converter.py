"""
A modul segédfüggvényeket biztosít az adattípusok felismeréséhez
és azok PostgreSQL-megfeleltetéséhez a táblalétrehozás során.
"""

# Alapvető típusmeghatározó függvény ha nem ismerjük.
def guess_type(key, value):
    if key == "value":
        return "string"
    if value is None:
        return "string"
    if isinstance(value, (int, float)):
        return "float"
    try:
        float(value)
        return "float"
    except:
        return "string"

# Python-szerű leképezéseket vagy általános adattípus megnevezéseket a tényleges PostgreSQL adattípusokra fordítja.
def map_to_postgresql_type(python_type: str) -> str:
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
    python_type_lower = python_type.lower().strip() if python_type else "string"
    if python_type and python_type[0].isupper():
        return python_type
    return type_mapping.get(python_type_lower, "TEXT")
