from typing import Dict, Type, Optional
from connectors.base import BaseConnector
from connectors.world_bank import WorldBankConnector
from connectors.who import WHOConnector
from connectors.f1 import F1Connector
from connectors.open_meteo import OpenMeteoConnector
from connectors.football_data import FootballDataConnector
from connectors.unirate import UniRateConnector

""" 
Ez a modul regisztrálja az összes elérhető API csatlakozót. 
Lehetővé teszi a csatlakozók típus szerinti lekérését és dinamikus példányosítását a rendszerben.
"""

# Connector típusok regisztálása
connector_registry: Dict[str, Type[BaseConnector]] = {
    "worldbank": WorldBankConnector,
    "world_bank": WorldBankConnector,
    "who": WHOConnector,
    "who_gho": WHOConnector,
    "f1_api": F1Connector,
    "open_meteo": OpenMeteoConnector,
    "football_data": FootballDataConnector,
    "unirate": UniRateConnector,
    "unirate_api": UniRateConnector,
}


# Egy példány létrehozása a megadott típusú csatlakozóból a megadott paraméterekkel
def get_connector(connector_type: str, **kwargs) -> BaseConnector:
    connector_class = connector_registry.get(connector_type.lower())
    if not connector_class:
        available = ", ".join(connector_registry.keys())
        raise ValueError(
            f"Unknown connector type: '{connector_type}'. "
            f"Available connectors: {available}"
        )

    return connector_class(**kwargs)


# Az új connector felvétele
def register_connector(name: str, connector_class: Type[BaseConnector]):
    connector_registry[name.lower()] = connector_class