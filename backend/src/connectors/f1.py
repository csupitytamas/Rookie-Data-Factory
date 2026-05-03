from typing import List, Dict, Any, Optional
import logging
import requests
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class F1Connector(BaseConnector):
    """
    Formula 1 API connector using OpenF1 (REST API).
    Only uses explicit meeting and session keys for stability.
    """

    # Végpontok, amik támogatják a driver_number szűrést
    DRIVER_SUPPORTED_ENDPOINTS = [
        "car_data", "drivers", "intervals", "laps", "location", "pit", "position", "stints"
    ]

    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "f1_api")
        base_url_fallback = kwargs.pop("base_url_fallback", "https://api.openf1.org/v1")
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, **kwargs)

    def get_filter_options(self, current_params: Dict[str, Any] = None) -> dict:
        current_params = current_params or {}
        
        def clean(key):
            val = current_params.get(key)
            if isinstance(val, dict): return val.get("value")
            if val in [None, "None", "undefined", "null", ""]: return None
            return val

        meeting_key = clean("meeting_key")
        session_key = clean("session_key")

        meetings_options = []
        sessions_options = []
        drivers_options = [{"label": "All Drivers", "value": ""}]

        try:
            # 1. MEETINGS lekérése (utolsó 50)
            m_resp = requests.get(f"{self.base_url_fallback}/meetings", timeout=5)
            if m_resp.status_code == 200:
                meetings = m_resp.json()
                for m in reversed(meetings[-50:]):
                    meetings_options.append({
                        "label": f"{m.get('year')} {m.get('meeting_official_name')}",
                        "value": str(m.get("meeting_key"))
                    })

            # Alapértelmezett meeting, ha nincs választva
            if not meeting_key and meetings_options:
                meeting_key = meetings_options[0]["value"]

            # 2. SESSIONS lekérése a választott meetinghez
            if meeting_key:
                s_resp = requests.get(f"{self.base_url_fallback}/sessions?meeting_key={meeting_key}", timeout=5)
                if s_resp.status_code == 200:
                    sessions = s_resp.json()
                    for s in reversed(sessions):
                        sessions_options.append({
                            "label": f"{s.get('session_name')} ({s.get('session_type')})",
                            "value": str(s.get("session_key"))
                        })

            # Alapértelmezett session, ha nincs választva
            if not session_key and sessions_options:
                session_key = sessions_options[0]["value"]

            # 3. DRIVERS lekérése a választott sessionhöz
            if session_key:
                d_resp = requests.get(f"{self.base_url_fallback}/drivers?session_key={session_key}", timeout=5)
                if d_resp.status_code == 200:
                    seen = set()
                    drivers_data = d_resp.json()
                    if isinstance(drivers_data, list):
                        for d in drivers_data:
                            num = str(d.get("driver_number"))
                            if num and num not in seen:
                                drivers_options.append({"label": f"{num} - {d.get('full_name')}", "value": num})
                                seen.add(num)
                        drivers_options[1:] = sorted(drivers_options[1:], key=lambda x: x["label"])

        except Exception as e:
            logger.error(f"Failed to dynamically load F1 options: {e}")

        return {
            "endpoint_type": {
                "type": "select",
                "required": True,
                "label": "F1 Data Type",
                "options": [
                    {"label": "Sessions", "value": "sessions"},
                    {"label": "Meetings", "value": "meetings"},
                    {"label": "Drivers", "value": "drivers"},
                    {"label": "Laps", "value": "laps"},
                    {"label": "Car Data (Telemetry)", "value": "car_data"},
                    {"label": "Location (GPS)", "value": "location"},
                    {"label": "Position", "value": "position"},
                    {"label": "Intervals", "value": "intervals"},
                    {"label": "Pit Stops", "value": "pit"},
                    {"label": "Stints (Tyres)", "value": "stints"},
                    {"label": "Weather", "value": "weather"}
                ],
                "default": "sessions"
            },
            "meeting_key": {
                "type": "select",
                "required": True,
                "label": "Meeting (Race Weekend)",
                "options": meetings_options,
                "default": meetings_options[0]["value"] if meetings_options else ""
            },
            "session_key": {
                "type": "select",
                "required": True,
                "label": "Session",
                "options": sessions_options,
                "default": sessions_options[0]["value"] if sessions_options else ""
            },
            "driver_number": {
                "type": "select",
                "required": False,
                "label": "Driver",
                "options": drivers_options,
                "default": ""
            }
        }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        def get_clean_value(key: str, default: str = ""):
            val = parameters.get(key, default)
            if isinstance(val, dict): return val.get("value", default)
            return val

        endpoint_type = get_clean_value("endpoint_type") or endpoint or "sessions"
        meeting_key = get_clean_value("meeting_key")
        session_key = get_clean_value("session_key")
        driver_number = get_clean_value("driver_number")

        query_params = []
        if session_key: 
            query_params.append(f"session_key={session_key}")
        elif meeting_key: 
            query_params.append(f"meeting_key={meeting_key}")
            
        if driver_number and endpoint_type in self.DRIVER_SUPPORTED_ENDPOINTS: 
            query_params.append(f"driver_number={driver_number}")

        url_path = endpoint_type
        if query_params: url_path += "?" + "&".join(query_params)
        return url_path

    def parse_response(self, response) -> List[Dict[str, Any]]:
        try:
            data = response.json() if hasattr(response, 'json') else response
            if isinstance(data, list): return data
            elif isinstance(data, dict): return [data]
            else: return []
        except:
            return []

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        url_path = self.build_url(endpoint, parameters)
        try:
            response = self.make_request(url_path)
            data = self.parse_response(response)
            
            if not data:
                raise ValueError(f"No data found for the selected filters.")
                
            return data
        except Exception as e:
            if "404" in str(e):
                raise ValueError(f"The F1 API returned 404 for this data. It might not exist yet.")
            raise e
