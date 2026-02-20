from typing import Dict, Type, Optional
from .base import BaseConnector
from .world_bank import WorldBankConnector
from .un_data import UNDataConnector
from .oecd import OECDConnector
from .who import WHOConnector

# Connector registry - itt regisztráljuk az összes elérhető connector-t
connector_registry: Dict[str, Type[BaseConnector]] = {
    "worldbank": WorldBankConnector,
    "world_bank": WorldBankConnector,
    "undata": UNDataConnector,
    "un_data": UNDataConnector,
    "oecd": OECDConnector,
    "who": WHOConnector,
    "who_gho": WHOConnector,

}


def get_connector(connector_type: str, **kwargs) -> BaseConnector:
    """
    Connector példány létrehozása típus alapján.
    
    Args:
        connector_type: A connector típusa (pl. "worldbank", "undata", stb.)
        **kwargs: Connector-specifikus inicializálási paraméterek
        
    Returns:
        BaseConnector példány
        
    Raises:
        ValueError: Ha a connector_type nem ismert
    """
    connector_class = connector_registry.get(connector_type.lower())
    
    if not connector_class:
        available = ", ".join(connector_registry.keys())
        raise ValueError(
            f"Unknown connector type: '{connector_type}'. "
            f"Available connectors: {available}"
        )
    
    return connector_class(**kwargs)


def register_connector(name: str, connector_class: Type[BaseConnector]):
    """
    Új connector regisztrálása runtime-ban.
    
    Args:
        name: A connector neve
        connector_class: A connector osztály
    """
    connector_registry[name.lower()] = connector_class

