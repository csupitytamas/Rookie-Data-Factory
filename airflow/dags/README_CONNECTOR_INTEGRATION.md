# Connector Réteg Integráció az Airflow-ban

## Áttekintés

Az Airflow DAG-ok mostantól használják a backend connector réteget az adatok lekéréséhez. Ez lehetővé teszi, hogy:

1. **Egységes adatlekérés**: Minden API forrás ugyanazt a connector interfészt használja
2. **Felhasználóbarát paraméterek**: A paraméterek automatikusan lefordítódnak API kódokká
3. **Backward compatibility**: A régi pipeline-ok továbbra is működnek

## Hogyan működik?

### 1. Connector Helper Modul

Az `airflow/dags/connector_helper.py` modul biztosítja a kapcsolatot a backend connector réteg és az Airflow DAG-ok között.

**Főbb funkciók:**
- `fetch_data_with_connector()`: Adatok lekérése connector használatával
- `fetch_data_legacy()`: Régi módszer (backward compatibility)

### 2. Extract Data Task Módosítása

Az `extract_data()` függvény az `etl_generate_dags.py`-ban most:

1. **Lekéri az API Schema információkat** az adatbázisból:
   - `connector_type`: Melyik connector-t használja
   - `endpoint`: Endpoint azonosító
   - `base_url`: Opcionális base URL
   - `field_mappings`: Mezőleképezések

2. **Lekéri a pipeline paramétereket** az `etlconfig` táblából:
   - `parameters`: Felhasználóbarát paraméterek (pl. `{"indicator": "population", "country": "hungary"}`)

3. **Connector használata** (ha van `connector_type`):
   - A `fetch_data_with_connector()` hívja meg a megfelelő connector-t
   - A paraméterek automatikusan lefordítódnak (pl. "population" -> "SP.POP.TOTL")
   - Az adatok normalizálva lesznek

4. **Fallback régi módszerre** (ha nincs `connector_type`):
   - Közvetlen URL lekérés `requests.get()`-tel
   - Backward compatibility a régi pipeline-okhoz

## Adatfolyam

```
Airflow DAG
    ↓
extract_data() task
    ↓
connector_helper.fetch_data_with_connector()
    ↓
Backend Connector (WorldBankConnector, UNDataConnector, stb.)
    ↓
ParameterTranslator.translate_params()  (felhasználóbarát -> API kódok)
    ↓
API hívás (World Bank, UN Data, OECD, WHO)
    ↓
Normalizált adatok visszaadása
    ↓
XCom push (extracted_data)
    ↓
transform_data() task
    ↓
load_data() task
```

## Példa

### Pipeline konfiguráció (etlconfig táblában):

```json
{
  "id": 1,
  "pipeline_name": "Hungary Population",
  "source": "worldbank",
  "parameters": {
    "indicator": "population",
    "country": "hungary",
    "date": "2020:2023"
  }
}
```

### API Schema (api_schemas táblában):

```json
{
  "source": "worldbank",
  "connector_type": "worldbank",
  "endpoint": "indicator",
  "base_url": "https://api.worldbank.org/v2"
}
```

### Folyamat:

1. **Airflow DAG futás**:
   - `extract_data(1)` task elindul
   - Lekéri a schema-t és paramétereket az adatbázisból

2. **Connector használata**:
   - `fetch_data_with_connector()` hívódik
   - `WorldBankConnector` inicializálódik
   - Paraméterek fordítása: `{"indicator": "population", ...}` -> `{"indicator": "SP.POP.TOTL", ...}`

3. **API hívás**:
   - URL építés: `https://api.worldbank.org/v2/country/HUN/indicator/SP.POP.TOTL?format=json&date=2020:2023`
   - Adatok lekérése és normalizálása

4. **Visszaadás**:
   - Normalizált adatok XCom-ba kerülnek
   - `transform_data()` és `load_data()` task-ok feldolgozzák

## Backward Compatibility

A régi pipeline-ok (amiknek nincs `connector_type` mezője) továbbra is működnek:

- Ha nincs `connector_type`, a `fetch_data_legacy()` hívódik
- Közvetlen URL lekérés a `source` mezőből
- Nincs paraméter fordítás

## Docker Környezet

Az Airflow Docker container-ben a backend/src könyvtár mountolva van:
- **Host path**: `../backend/src`
- **Container path**: `/opt/backend/src`

A `connector_helper.py` automatikusan megtalálja ezt a path-ot és importálja a connector-öket.

## Hibakeresés

Ha problémák vannak a connector importtal:

1. **Ellenőrizd a Docker volume mount-ot**:
   ```yaml
   volumes:
     - ../backend/src:/opt/backend/src
   ```

2. **Nézd meg az Airflow logokat**:
   - A `extract_data` task logjában látszódik, hogy melyik módszert használja
   - `[EXTRACT_DATA] Using connector: worldbank` - connector használat
   - `[EXTRACT_DATA] Using legacy method` - régi módszer

3. **Teszteld a connector-öket**:
   ```python
   from connector_helper import fetch_data_with_connector
   
   data = fetch_data_with_connector(
       connector_type="worldbank",
       endpoint="indicator",
       parameters={"indicator": "population", "country": "hungary"}
   )
   ```

## Új Connector Hozzáadása

1. **Backend-ben**: Hozz létre egy új connector osztályt (pl. `MyConnector`)
2. **Registry-ben**: Regisztráld a `connector_registry`-ben
3. **Airflow-ban**: Automatikusan elérhető lesz, nincs további módosítás szükséges!



