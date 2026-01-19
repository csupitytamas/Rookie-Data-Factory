from typing import List, Dict, Any, Optional
import requests
from .base import BaseConnector


class WorldBankConnector(BaseConnector):
    """
    World Bank API connector.
    API dokumentáció: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
    """

    def __init__(self, **kwargs):
        base_url = kwargs.pop("base_url", "https://api.worldbank.org/v2")
        super().__init__(base_url=base_url, **kwargs)
        self.session.headers.update({
            "Accept": "application/json"
        })

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        World Bank URL építése.
        Példa: /country/{country}/indicator/{indicator}?format=json&date={date}
        """
        country = parameters.get("country", "all")
        indicator = parameters.get("indicator")
        date = parameters.get("date", "")
        per_page = parameters.get("per_page", 1000)
        page = parameters.get("page", 1)

        if not indicator:
            raise ValueError("World Bank connector requires 'indicator' parameter")

        # World Bank API formátum: /country/{country}/indicator/{indicator}
        url = f"{self.base_url}/country/{country}/indicator/{indicator}"
        
        # Query paraméterek
        params = {
            "format": "json",
            "per_page": per_page,
            "page": page
        }
        
        if date:
            params["date"] = date

        # URL építése query string-gel
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{url}?{query_string}"

    def parse_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        """
        World Bank JSON válasz parzolása.
        A World Bank API 2 elemű tömböt ad vissza:
        [0] = metadata (pagination info)
        [1] = data array
        """
        data = response.json()
        
        if not isinstance(data, list) or len(data) < 2:
            return []

        metadata = data[0] if len(data) > 0 else {}
        records = data[1] if len(data) > 1 else []

        # Normalizálás standard formátumba
        normalized = []
        for record in records:
            normalized.append({
                "country": record.get("country", {}).get("value", ""),
                "country_code": record.get("country", {}).get("id", ""),
                "indicator": record.get("indicator", {}).get("value", ""),
                "indicator_code": record.get("indicator", {}).get("id", ""),
                "value": record.get("value"),
                "year": record.get("date", ""),
                "decimal": record.get("decimal", 0),
            })

        return normalized

    def fetch(
        self,
        endpoint: str,
        parameters: Dict[str, Any],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        World Bank adatok lekérése lapozással.
        """
        all_data = []
        page = 1
        per_page = parameters.get("per_page", 1000)

        while True:
            params = parameters.copy()
            params["page"] = page
            params["per_page"] = per_page

            url = self.build_url(endpoint, params)
            response = self.make_request(url)
            data = self.parse_response(response)

            if not data:
                break

            all_data.extend(data)

            # Ellenőrizzük, van-e még oldal
            # A World Bank API metadata-ban van a total oldalak száma
            try:
                metadata = response.json()[0] if response.json() else {}
                total_pages = metadata.get("pages", 1)
                if page >= total_pages:
                    break
            except:
                # Ha nincs metadata, akkor csak egy oldal van
                break

            page += 1

        return all_data

