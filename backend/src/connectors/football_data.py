from typing import List, Dict, Any, Optional
import requests
import logging
import os
from functools import lru_cache
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

@lru_cache(maxsize=64)
def _cached_football_get(url: str, headers_tuple: tuple, params_tuple: tuple):
    headers = dict(headers_tuple)
    params = dict(params_tuple)
    logger.info(f"Football API Request (NOT CACHED): {url}")
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

class FootballDataConnector(BaseConnector):
    """
    Football-Data.org API connector (v4).
    """

    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "football_data_api")
        # Read API Key from environment variable, fallback to provide by user if not found
        self.api_key = os.getenv("FOOTBALL_DATA_API_KEY", "eb2edc0dabd94891a5ef65d438ac8e8e")
        base_url_fallback = kwargs.pop("base_url_fallback", "https://api.football-data.org/v4")
        # Enable hook now that we've added the connection to .env
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, use_hook=True, **kwargs)

    def get_headers(self) -> Dict[str, str]:
        return {
            "X-Auth-Token": self.api_key,
            "Content-Type": "application/json"
        }

    def get_filter_options(self, current_params: Dict[str, Any] = None) -> dict:
        competition_options = []
        try:
            url = "https://api.football-data.org/v4/competitions"
            headers_tuple = tuple(sorted(self.get_headers().items()))
            data = _cached_football_get(url, headers_tuple, tuple())
            
            competitions = data.get("competitions", [])
            for comp in competitions:
                plan = comp.get("plan")
                if plan == "TIER_ONE":
                    label = f"{comp.get('name')} ({comp.get('area', {}).get('name')})"
                    competition_options.append({
                        "label": label,
                        "value": str(comp.get("code"))
                    })
            competition_options.sort(key=lambda x: x["label"])
        except Exception as e:
            logger.error(f"Error fetching competitions: {e}")

        if not competition_options:
            competition_options = [
                {"label": "Premier League (PL)", "value": "PL"},
                {"label": "Bundesliga (BL1)", "value": "BL1"},
                {"label": "Eredivisie (ED)", "value": "ED"},
                {"label": "Primera Division (PD)", "value": "PD"},
                {"label": "Serie A (SA)", "value": "SA"},
                {"label": "Ligue 1 (FL1)", "value": "FL1"}
            ]

        return {
            "competition": {
                "type": "select",
                "label": "Select League",
                "description": "Available leagues",
                "required": True,
                "options": competition_options,
                "default": "PL"
            }
        }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        comp = parameters.get("competition", "PL")
        if isinstance(comp, dict): comp = comp.get("value")
        return f"competitions/{comp}/matches"

    def parse_response(self, response) -> List[Dict[str, Any]]:
        try:
            # Handle both requests response and Airflow Hook response
            if hasattr(response, 'json'):
                data = response.json()
            else:
                import json
                data = json.loads(response)

            matches = data.get("matches", [])
            if not matches and "message" in data:
                raise ValueError(f"API Message: {data['message']}")

            records = []
            for m in matches:
                records.append({
                    "utc_date": m.get("utcDate"),
                    "status": m.get("status"),
                    "matchday": m.get("matchday"),
                    "home_team": m.get("homeTeam", {}).get("name"),
                    "away_team": m.get("awayTeam", {}).get("name"),
                    "score_home": m.get("score", {}).get("fullTime", {}).get("home"),
                    "score_away": m.get("score", {}).get("fullTime", {}).get("away"),
                    "winner": m.get("score", {}).get("winner"),
                    "competition": m.get("competition", {}).get("name")
                })
            return records
        except Exception as e:
            if "429" in str(e):
                raise ValueError("Rate Limit reached. Please wait 60 seconds.")
            raise e

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        url_path = self.build_url(endpoint, parameters)
        resp = self.make_request(url_path, headers=self.get_headers())
        return self.parse_response(resp)
