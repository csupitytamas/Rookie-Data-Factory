import requests
from typing import Dict, Any, List
from .base import BaseConnector
import re

class WHOConnector(BaseConnector):

    def __init__(self, **kwargs):
        base_url = kwargs.pop("base_url", "https://ghoapi.azureedge.net/api")
        super().__init__(base_url=base_url, **kwargs)

        # 🔥 Betöltjük egyszer az összes valós entity set-et
        self.valid_entities = self._load_valid_entities()

    def _load_valid_entities(self) -> List[str]:
        """Lekéri a WHO metaadat XML-t és kilistázza a valós entity neveket."""
        xml = requests.get(f"{self.base_url}/$metadata").text
        entities = re.findall(r'<EntitySet Name="([^"]+)"', xml)
        return entities

    def resolve_entity(self, indicator: str) -> str:
        """Megkeresi, hogy a WHO indikátorhoz tartozik-e valódi entity set."""

        # ✔️ Ha maga az indikátor entity set neve (pl. RSUD_620)
        if indicator in self.valid_entities:
            return indicator

        # ✔️ HA NEM létezik entity — 100% biztosan nincs mögötte adat
        raise ValueError(
            f"WHO indicator '{indicator}' nem rendelkezik adat entitással a WHO API-ban."
        )

    def get_filter_options(self):
        """
        A frontend dropdown számára visszaadjuk CSAK a valós WHO entity indikátorokat.
        """
        url = f"{self.base_url}/Indicator"
        resp = self.make_request(url)
        indicators_list = resp.json().get("value", [])

        # 🔥 SZŰRÉS — csak azokat engedjük, amelyek mögött VAN entity set
        options = []
        for item in indicators_list:
            code = item.get("IndicatorCode")
            if code in self.valid_entities:
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

        return f"{self.base_url}/{entity}"

    def parse_response(self, response):
        rows = response.json().get("value", [])
        out = []
        for row in rows:
            flat = {}
            for key, val in row.items():
                safe_key = key.lower().replace(" ", "_")
                if safe_key == "id":
                    safe_key = "who_id"
                flat[safe_key] = val

            flat["country"] = flat.get("spatialdim")
            flat["year"] = flat.get("timedim")
            flat["indicator"] = flat.get("indicatorcode")

            out.append(flat)

        return out

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs):
        url = self.build_url(endpoint, parameters)
        resp = self.make_request(url)
        return self.parse_response(resp)
