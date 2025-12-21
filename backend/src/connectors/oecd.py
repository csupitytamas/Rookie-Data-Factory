from typing import List, Dict, Any, Optional
import requests
from .base import BaseConnector


class OECDConnector(BaseConnector):
    """
    OECD API connector.
    OECD.Stat API dokumentáció: https://stats.oecd.org/
    """

    def __init__(self, **kwargs):
        base_url = kwargs.pop("base_url", "https://stats.oecd.org/SDMX-JSON/data")
        super().__init__(base_url=base_url, **kwargs)
        self.session.headers.update({
            "Accept": "application/json"
        })

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        OECD URL építése.
        Példa: /{dataset}/{filter}?contentType=json
        """
        dataset = parameters.get("dataset")
        if not dataset:
            raise ValueError("OECD connector requires 'dataset' parameter")

        # OECD API formátum: /{dataset}/{filter}
        filter_str = parameters.get("filter", "all")
        
        url = f"{self.base_url}/{dataset}/{filter_str}"
        
        # Query paraméterek
        params = {
            "contentType": "json"
        }
        
        if "startTime" in parameters:
            params["startTime"] = parameters["startTime"]
        if "endTime" in parameters:
            params["endTime"] = parameters["endTime"]

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{url}?{query_string}"

    def parse_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        """
        OECD SDMX-JSON válasz parzolása.
        Az OECD SDMX formátumot használ, ami komplex struktúrát tartalmaz.
        """
        data = response.json()
        
        if not data or "dataSets" not in data:
            return []

        # SDMX struktúra feldolgozása
        datasets = data.get("dataSets", [])
        structure = data.get("structure", {})
        dimensions = structure.get("dimensions", {})
        observations = datasets[0].get("observations", {}) if datasets else {}

        # Dimenziók értékeinek kinyerése
        dim_keys = list(dimensions.keys())
        dim_values = {}
        for dim_key in dim_keys:
            dim_values[dim_key] = [
                item.get("id", "") for item in dimensions.get(dim_key, {}).get("values", [])
            ]

        # Normalizálás
        normalized = []
        for obs_key, obs_value in observations.items():
            # Az obs_key egy index vagy kulcs, ami a dimenziók kombinációját jelöli
            # Az OECD API-ban ez általában egy szám vagy tuple
            
            record = {}
            
            # Dimenziók értékeinek hozzáadása
            for i, dim_key in enumerate(dim_keys):
                if isinstance(obs_key, (list, tuple)) and i < len(obs_key):
                    idx = obs_key[i]
                    if idx < len(dim_values[dim_key]):
                        record[dim_key.lower()] = dim_values[dim_key][idx]
                elif isinstance(obs_key, str):
                    # Ha string kulcs, akkor parse-olni kell
                    parts = obs_key.split(":")
                    if i < len(parts):
                        idx = int(parts[i]) if parts[i].isdigit() else 0
                        if idx < len(dim_values[dim_key]):
                            record[dim_key.lower()] = dim_values[dim_key][idx]

            # Érték hozzáadása
            if isinstance(obs_value, list) and len(obs_value) > 0:
                record["value"] = obs_value[0]
            else:
                record["value"] = obs_value

            normalized.append(record)

        return normalized

    def fetch(
        self,
        endpoint: str,
        parameters: Dict[str, Any],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        OECD adatok lekérése.
        """
        url = self.build_url(endpoint, parameters)
        response = self.make_request(url)
        return self.parse_response(response)

