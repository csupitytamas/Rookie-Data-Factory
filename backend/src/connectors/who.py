from typing import Dict, Any, List
import re
import logging
from .base import BaseConnector

logger = logging.getLogger(__name__)

class WHOConnector(BaseConnector):
    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "who_api")
        # Beadjuk a fallback URL-t a backend kedvéért
        base_url_fallback = kwargs.pop("base_url_fallback", "https://ghoapi.azureedge.net/api")
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, **kwargs)
        
        self.valid_entities = []
        # Most már a Backend is biztonságosan le tudja futtatni a fallback segítségével!
        try:
            self.valid_entities = self._load_valid_entities()
        except Exception as e:
            logger.warning(f"Nem sikerült betölteni a WHO entitásokat: {e}")

    def _load_valid_entities(self) -> List[str]:
        response = self.make_request("$metadata")
        xml = response.text
        return re.findall(r'<EntitySet Name="([^"]+)"', xml)

    def resolve_entity(self, indicator: str) -> str:
        """
        Meghatározza a végpontot. Ha az indikátor nem saját tábla, 
        akkor a Fact táblát használja szűréssel.
        """
        if indicator in self.valid_entities:
            return indicator
        # A WHO API legtöbb adata a Fact entitás alatt érhető el
        return "Fact"

    def get_filter_options(self):
        """Visszaadja az összes elérhető indikátort a frontendnek."""
        # Nincs f"{self.base_url}/Indicator", csak "Indicator"
        resp = self.make_request("Indicator")
        indicators_list = resp.json().get("value", [])

        options = []
        for item in indicators_list:
            code = item.get("IndicatorCode")
            options.append({
                "label": item.get("IndicatorName", code),
                "value": code
            })

        return {
            "indicator": {
                "type": "select",
                "required": True,
                "label": "Indicator",
                "description": "Select WHO GHO indicator code",
                "options": options
            }
        }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        indicator = parameters.get("indicator")
        if not indicator:
            raise ValueError("WHO connector requires 'indicator' parameter")

        entity = self.resolve_entity(indicator)
        
        # CSAK A RELATÍV ÚTVONAL: nem kell a base_url
        url = f"{entity}"
        if entity == "Fact":
            url += f"?$filter=IndicatorCode eq '{indicator}'"
            
        return url

    def parse_response(self, response):
        rows = response.json().get("value", [])
        out = []
        for row in rows:
            flat = {key.lower().replace(" ", "_"): val for key, val in row.items()}
            
            # Kulcsok egységesítése
            if "id" in flat: flat["who_id"] = flat.pop("id")
            
            flat["country"] = flat.get("spatialdim")
            flat["year"] = flat.get("timedim")
            flat["value"] = flat.get("numericvalue")
            flat["indicator"] = flat.get("indicatorcode")

            out.append(flat)
        return out

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs):
        # A build_url most már a relatív URL-t adja vissza
        url = self.build_url(endpoint, parameters)
        resp = self.make_request(url)
        return self.parse_response(resp)