from sqlalchemy import text
from sqlalchemy.orm import Session
import re
import uuid

""" 
Segédfüggvények az adatbázis táblanevek létrehozáshoz, 
az Airflow DAG azonosítók generálásához, 
valamint a nevek biztonságos formázásához. 
"""

# Megtisztítja a bemeneti karakterláncot: kisbetűssé alakítja, a speciális karaktereket alulvonásra cseréli, és maximalizálja a hosszát.
def sanitize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    return name[:40]

# Létrehoz egy egyedi táblanevet a pipeline neve, verziója és egy rövid UUID alapján.
def generate_table_name(pipeline_name: str, version: int, uid: str | None = None) -> str:
    pipeline_name = sanitize_name(pipeline_name)
    uid = uid or str(uuid.uuid4())[:8]
    return f"{pipeline_name}_v{version}_{uid}"

# Létrehoz egy érvényes és egyedi azonosítót az Airflow DAG számára a pipeline neve és verziója alapján.
def generate_dag_id(pipeline_name: str, version: int) -> str:
    sanitized = remove_version_suffix(pipeline_name)
    sanitized = sanitized.lower().replace(' ', '_')
    return f"{sanitized}_v{version}"

# Eltávolítja a verziójelzést (pl. ' v2') a új verziójú pipeline név végéről.
def remove_version_suffix(name: str) -> str:
    return re.sub(r' v\d+$', '', name)