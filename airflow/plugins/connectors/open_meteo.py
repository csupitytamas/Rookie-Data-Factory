from typing import List, Dict, Any, Optional
import requests
import logging
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class OpenMeteoConnector(BaseConnector):
    """
    Open-Meteo API connector - Free weather data.
    Documentation: https://open-meteo.com/
    """

    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "open_meteo_api")
        base_url_fallback = kwargs.pop("base_url_fallback", "https://api.open-meteo.com/v1")
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, **kwargs)

    def get_filter_options(self, current_params: Dict[str, Any] = None) -> dict:
        """
        Provides filter options including city search results if a city is provided.
        """
        current_params = current_params or {}
        city_query = current_params.get("city_search")
        
        city_options = []
        if city_query and len(city_query) > 2:
            try:
                # Geocoding API hívás a város kereséséhez
                geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_query}&count=5&language=en&format=json"
                resp = requests.get(geo_url, timeout=5)
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    for res in results:
                        label = f"{res.get('name')}, {res.get('country')} ({res.get('admin1', '')})"
                        # Az értékben eltároljuk a koordinátákat JSON-szerűen
                        value = f"{res.get('latitude')},{res.get('longitude')}"
                        city_options.append({"label": label, "value": value})
            except Exception as e:
                logger.error(f"Geocoding error: {e}")

        return {
            "city_search": {
                "type": "text",
                "label": "Search City",
                "description": "Type at least 3 characters to search...",
                "required": False
            },
            "location": {
                "type": "select",
                "label": "Select Location",
                "description": "Choose a city from the search results",
                "required": True,
                "options": city_options if city_options else [{"label": "Search for a city first", "value": ""}]
            },
            "weather_vars": {
                "type": "select",
                "label": "Weather Variables",
                "description": "Select data to include",
                "required": True,
                "options": [
                    {"label": "Temperature (2m)", "value": "temperature_2m"},
                    {"label": "Relative Humidity", "value": "relative_humidity_2m"},
                    {"label": "Apparent Temperature", "value": "apparent_temperature"},
                    {"label": "Precipitation", "value": "precipitation"},
                    {"label": "Wind Speed (10m)", "value": "wind_speed_10m"},
                    {"label": "Cloud Cover", "value": "cloud_cover"}
                ],
                "default": "temperature_2m"
            },
            "forecast_days": {
                "type": "select",
                "label": "Forecast Range",
                "options": [
                    {"label": "1 Day", "value": "1"},
                    {"label": "3 Days", "value": "3"},
                    {"label": "7 Days", "value": "7"},
                    {"label": "14 Days", "value": "14"}
                ],
                "default": "7"
            }
        }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        location = parameters.get("location")
        if isinstance(location, dict): location = location.get("value")
        
        if not location or ',' not in str(location):
            raise ValueError("Please select a valid location from the list.")
            
        lat, lon = str(location).split(',')
        vars = parameters.get("weather_vars", "temperature_2m")
        if isinstance(vars, dict): vars = vars.get("value")
        
        days = parameters.get("forecast_days", "7")
        if isinstance(days, dict): days = days.get("value")
        
        # Build the final Open-Meteo URL
        return f"forecast?latitude={lat}&longitude={lon}&hourly={vars}&forecast_days={days}"

    def parse_response(self, response) -> List[Dict[str, Any]]:
        data = response.json()
        if "hourly" not in data:
            return []
            
        hourly = data["hourly"]
        times = hourly.get("time", [])
        
        # Átfordítjuk oszlop-folyamból sor-folyamra (ETL kompatibilis)
        records = []
        for i, t in enumerate(times):
            record = {"time": t, "latitude": data.get("latitude"), "longitude": data.get("longitude")}
            for key in hourly.keys():
                if key != "time":
                    record[key] = hourly[key][i]
            records.append(record)
            
        return records

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        url_path = self.build_url(endpoint, parameters)
        resp = self.make_request(url_path)
        return self.parse_response(resp)
