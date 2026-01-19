from sqlalchemy import Table, MetaData, insert
from sqlalchemy.orm import Session
from src.models.etl_config_model import ETLConfig
from src.models.api_schemas_model import APISchema
from src.connectors import get_connector
import logging

logger = logging.getLogger(__name__)


def load_to_target_table(pipeline_id: int, db: Session):
    """
    Pipeline adatok betöltése a cél táblába connector réteg használatával.
    """
    # 1. Pipeline lekérése
    pipeline = db.query(ETLConfig).filter_by(id=pipeline_id).first()
    if not pipeline:
        raise Exception(f"Pipeline {pipeline_id} not found")

    if not pipeline.target_table_name:
        raise Exception("Target table name is missing.")

    # 2. API Schema lekérése
    api_schema = db.query(APISchema).filter_by(source=pipeline.source).first()
    if not api_schema:
        raise Exception(f"API schema not found for source: {pipeline.source}")

    # 3. Connector kiválasztása és inicializálása
    connector_type = api_schema.connector_type
    
    # Ha nincs connector_type, akkor fallback a régi módszerre (backward compatibility)
    if not connector_type:
        logger.warning(
            f"No connector_type specified for source {pipeline.source}. "
            "Using legacy direct URL method."
        )
        _load_legacy_method(pipeline, db)
        return

    try:
        # Connector inicializálása
        connector_kwargs = {}
        if api_schema.base_url:
            connector_kwargs["base_url"] = api_schema.base_url

        connector = get_connector(connector_type, **connector_kwargs)

        # 4. Paraméterek összeállítása
        # Az ETLConfig parameters mezőjéből, vagy üres dict
        # A paramétereket közvetlenül használjuk (nincs fordítás)
        parameters = pipeline.parameters or {}
        
        logger.info(
            f"Using parameters: {parameters}"
        )
        
        # Endpoint meghatározása
        endpoint = api_schema.endpoint or "default"

        # 5. Adatok lekérése connector-ral
        logger.info(
            f"Fetching data using connector '{connector_type}' "
            f"with parameters: {parameters}"
        )
        
        with connector:
            data = connector.fetch(endpoint, parameters)

        if not data:
            logger.warning(f"No data returned from connector for pipeline {pipeline_id}")
            return

        # 6. Adatok normalizálása field_mappings alapján (ha van)
        if pipeline.field_mappings:
            data = connector.normalize_data(data, pipeline.field_mappings)

        # 7. Mezők nevei a field_mappings-ből vagy az adatok kulcsai
        if pipeline.field_mappings:
            fields = [f["name"] if isinstance(f, dict) else f for f in pipeline.field_mappings]
        else:
            # Ha nincs field_mapping, akkor az összes mezőt használjuk
            fields = list(data[0].keys()) if data else []

        # 8. Adatok előkészítése: list[dict] formában
        insert_data = []
        for row in data:
            record = {}
            for col in fields:
                record[col] = row.get(col)
            insert_data.append(record)

        # 9. Dinamikus táblahivatkozás a MetaData segítségével
        metadata = MetaData()
        table = Table(pipeline.target_table_name, metadata, autoload_with=db.bind)

        # 10. SQLAlchemy insert (tömeges beszúrás)
        stmt = insert(table).values(insert_data)
        db.execute(stmt)
        db.commit()

        logger.info(
            f"{len(insert_data)} sor betöltve a(z) {pipeline.target_table_name} táblába."
        )

    except Exception as e:
        logger.error(f"Error loading data with connector: {e}", exc_info=True)
        raise Exception(f"Failed to load data: {str(e)}")


def _load_legacy_method(pipeline: ETLConfig, db: Session):
    """
    Régi módszer: közvetlen URL lekérés (backward compatibility).
    """
    import requests
    
    # API lekérés
    response = requests.get(pipeline.source)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        data = [data]

    # Mezők nevei a field_mappings-ből
    if pipeline.field_mappings:
        fields = [f["name"] if isinstance(f, dict) else f for f in pipeline.field_mappings]
    else:
        fields = list(data[0].keys()) if data else []

    # Adatok előkészítése: list[dict] formában
    insert_data = []
    for row in data:
        insert_data.append({col: row.get(col) for col in fields})

    # Dinamikus táblahivatkozás a MetaData segítségével
    metadata = MetaData()
    table = Table(pipeline.target_table_name, metadata, autoload_with=db.bind)

    # SQLAlchemy insert (tömeges beszúrás)
    stmt = insert(table).values(insert_data)
    db.execute(stmt)
    db.commit()

    logger.info(f"{len(insert_data)} sor betöltve a(z) {pipeline.target_table_name} táblába (legacy method).")