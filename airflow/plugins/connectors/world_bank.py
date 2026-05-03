from typing import List, Dict, Any, Optional
import requests
from connectors.base import BaseConnector


class WorldBankConnector(BaseConnector):
    """
    World Bank API connector.
    Támogatja az indikátorokat és a témaköröket (topics).
    API dokumentáció: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
    """

    def __init__(self, **kwargs):
        # Alapértelmezett URL beállítása
        base_url = kwargs.pop("base_url", "https://api.worldbank.org/v2")
        super().__init__(base_url=base_url, **kwargs)
        if not self.hook:  # Ha nem Airflow módban fut
            self.session.headers.update({
                "Accept": "application/json"
            })

    def get_filter_options(self) -> dict:
        """
        Lekéri az elérhető témaköröket a UI szűrőhöz.
        """
        try:
            # A /topic végpont lekérése JSON formátumban
            response = self.make_request("topic?format=json")
            data = response.json()
            if isinstance(data, list) and len(data) > 1:
                return {
                    "topics": [
                        {"id": item.get("id"), "value": item.get("value")}
                        for item in data[1]
                    ]
                }
        except Exception as e:
            print(f"Hiba a témakörök lekérésekor: {e}")
        return {"topics": []}

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        World Bank URL építése dinamikusan.
        """
        per_page = parameters.get("per_page", 1000)
        page = parameters.get("page", 1)

        # 1. Speciális eset: Témakörök listázása
        if endpoint == "topic":
            return f"{self.base_url}/topic?format=json&per_page={per_page}&page={page}"

        # 2. Alapértelmezett eset: Indikátor adatok
        country = parameters.get("country", "all")
        indicator = parameters.get("indicator")
        date = parameters.get("date", "")

        if not indicator:
            raise ValueError("World Bank connector requires 'indicator' or 'topic' endpoint")

        url = f"{self.base_url}/country/{country}/indicator/{indicator}"

        params = {
            "format": "json",
            "per_page": per_page,
            "page": page
        }

        if date:
            params["date"] = date

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{url}?{query_string}"

    def parse_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        """
        World Bank JSON válasz parzolása és normalizálása.
        """
        try:
            data = response.json()
        except Exception:
            return []

        if not isinstance(data, list) or len(data) < 2:
            return []

        records = data[1] if len(data) > 1 else []
        normalized = []

        for record in records:
            # Ellenőrizzük, hogy topic vagy indicator adatról van-e szó
            if "sourceNote" in record:  # Ez egy Topic rekord
                normalized.append({
                    "id": record.get("id"),
                    "value": record.get("value"),
                    "description": record.get("sourceNote", "")
                })
            else:  # Ez egy standard statisztikai rekord
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

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Adatok lekérése automatikus lapozással.
        """
        all_data = []
        page = parameters.get("page", 1)
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

            # Lapozás ellenőrzése a metaadatok alapján
            try:
                metadata = response.json()[0] if response.json() else {}
                total_pages = metadata.get("pages", 1)
                if page >= total_pages:
                    break
            except:
                break

            page += 1

        return all_data