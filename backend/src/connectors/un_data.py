from typing import List, Dict, Any, Optional
import requests
import csv
import io
from .base import BaseConnector


class UNDataConnector(BaseConnector):
    """
    UN Data API connector.
    UN Data API CSV formátumban adja vissza az adatokat.
    """

    def __init__(self, **kwargs):
        base_url = kwargs.pop("base_url", "https://data.un.org/ws/rest/data")
        super().__init__(base_url=base_url, **kwargs)
        self.session.headers.update({
            "Accept": "text/csv"
        })

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        UN Data URL építése.
        Példa: /data/flow/{flow}/freq/{freq}/refArea/{refArea}/...
        """
        # UN Data API RESTful endpoint-okat használ
        # A paramétereket path paraméterekként vagy query paraméterekként kell átadni
        
        # Alap URL
        url = f"{self.base_url}/{endpoint}" if endpoint else self.base_url
        
        # Query paraméterek
        query_params = {}
        
        # Gyakori paraméterek
        if "flow" in parameters:
            query_params["flow"] = parameters["flow"]
        if "freq" in parameters:
            query_params["freq"] = parameters["freq"]
        if "refArea" in parameters:
            query_params["refArea"] = parameters["refArea"]
        if "indicator" in parameters:
            query_params["indicator"] = parameters["indicator"]
        if "timePeriod" in parameters:
            query_params["timePeriod"] = parameters["timePeriod"]
        
        # Egyéb paraméterek
        for key, value in parameters.items():
            if key not in ["flow", "freq", "refArea", "indicator", "timePeriod"]:
                query_params[key] = value

        if query_params:
            query_string = "&".join([f"{k}={v}" for k, v in query_params.items()])
            url = f"{url}?{query_string}"

        return url

    def parse_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        """
        UN Data CSV válasz parzolása.
        A CSV-t dict listává alakítja.
        """
        # CSV tartalom olvasása
        content = response.text
        
        if not content or not content.strip():
            return []

        # CSV parzolás
        csv_reader = csv.DictReader(io.StringIO(content))
        records = list(csv_reader)

        # Normalizálás standard formátumba
        normalized = []
        for record in records:
            # A mezőneveket standardizáljuk
            normalized_record = {}
            
            # Keresés a lehetséges mezőnevekben
            country_fields = ["Country", "Country or Area", "refArea", "REF_AREA"]
            indicator_fields = ["Indicator", "Indicator Name", "indicator", "INDICATOR"]
            value_fields = ["Value", "value", "OBS_VALUE"]
            year_fields = ["Year", "Time Period", "timePeriod", "TIME_PERIOD", "TIME"]
            
            for field_list, target_key in [
                (country_fields, "country"),
                (indicator_fields, "indicator"),
                (value_fields, "value"),
                (year_fields, "year")
            ]:
                for field in field_list:
                    if field in record:
                        normalized_record[target_key] = record[field]
                        break
            
            # Minden egyéb mezőt is hozzáadjuk
            for key, value in record.items():
                if key not in country_fields + indicator_fields + value_fields + year_fields:
                    normalized_record[key.lower().replace(" ", "_")] = value

            normalized.append(normalized_record)

        return normalized

    def fetch(
        self,
        endpoint: str,
        parameters: Dict[str, Any],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        UN Data adatok lekérése.
        """
        url = self.build_url(endpoint, parameters)
        response = self.make_request(url)
        return self.parse_response(response)

