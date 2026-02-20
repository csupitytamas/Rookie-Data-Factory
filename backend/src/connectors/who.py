import requests
from typing import Dict, Any, List
from .base import BaseConnector
import re

class WHOConnector(BaseConnector):
    def __init__(self, **kwargs):
        base_url = kwargs.pop("base_url", "https://ghoapi.azureedge.net/api")
        super().__init__(base_url=base_url, **kwargs)
        # Ezt megtarthatjuk tájékoztató jelleggel, de nem szűrünk vele szigorúan
        try:
            self.valid_entities = self._load_valid_entities()
        except:
            self.valid_entities = []

    def _load_valid_entities(self) -> List[str]:
        """Lekéri a WHO metaadat XML-t."""
        xml = requests.get(f"{self.base_url}/$metadata", timeout=10).text
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
        url = f"{self.base_url}/Indicator"
        resp = self.make_request(url)
        indicators_list = resp.json().get("value", [])

        options = []
        for item in indicators_list:
            code = item.get("IndicatorCode")
            # ELTÁVOLÍTVA: a szigorú 'if code in self.valid_entities' szűrés
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
        
        # Ha a Fact táblát használjuk, hozzáadjuk az OData szűrést az indikátorra
        url = f"{self.base_url}/{entity}"
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
        url = self.build_url(endpoint, parameters)
        resp = self.make_request(url)
        return self.parse_response(resp)