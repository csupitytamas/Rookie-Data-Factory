from typing import List, Dict, Any, Optional
import logging
import requests
import os
import json
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class OECDConnector(BaseConnector):
    """
    OECD API connector - With automated dataflow discovery and caching.
    """

    _cache_file = os.path.join(os.path.dirname(__file__), "oecd_dataflows.json")

    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "oecd_api")
        base_url_fallback = kwargs.pop("base_url_fallback", "https://sdmx.oecd.org/public/rest")
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, **kwargs)

    def _get_cached_dataflows(self) -> Optional[List[Dict[str, str]]]:
        if os.path.exists(self._cache_file):
            try:
                with open(self._cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data and len(data) > 5: # Csak ha tényleg megvan a nagy lista
                        return data
            except Exception as e:
                logger.error(f"Error reading OECD cache: {e}")
        return None

    def _save_dataflows_to_cache(self, dataflows: List[Dict[str, str]]):
        try:
            with open(self._cache_file, "w", encoding="utf-8") as f:
                json.dump(dataflows, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error writing OECD cache: {e}")

    def get_filter_options(self, current_params: Dict[str, Any] = None) -> dict:
        options = self._get_cached_dataflows()

        if not options:
            logger.info("OECD dataflow cache empty or too small, fetching from API (this may take 20-30s)...")
            fallback_options = [
                {"label": "Average annual hours actually worked per worker", "value": "OECD.ELS.SAE,DSD_HW@DF_AVG_ANN_HRS_WKD,1.0/all"},
                {"label": "Average annual wages", "value": "OECD.ELS.SAE,DSD_EARNINGS@AV_AN_WAGE,1.0/all"},
                {"label": "Satisfaction with public services", "value": "OECD.GOV.GIP,DSD_GOV_INT@DF_GOV_SPS_2025,1.1/all"},
                {"label": "Trade in Value Added (TiVA) 2023", "value": "OECD.STI.PIE,DSD_TIVA_2023@DF_TIVA_2023,1.0/all"}
            ]
            options = fallback_options
            
            try:
                url = f"{self.base_url_fallback}/dataflow/all/all/latest"
                headers = {"Accept": "application/json, text/plain, */*"}
                resp = requests.get(url, headers=headers, timeout=45)
                
                if resp.status_code == 200:
                    data = resp.json()
                    fetched = []
                    dataflows = data.get("data", {}).get("dataflows", [])
                    for df in dataflows:
                        df_id = df.get("id")
                        agency = df.get("agencyID")
                        version = df.get("version", "1.0")
                        name = df.get("name", df_id)
                        
                        if df_id and agency:
                            value = f"{agency},{df_id},{version}/all"
                            fetched.append({"label": name, "value": value})
                    
                    if len(fetched) > 10:
                        options = sorted(fetched, key=lambda x: x["label"])
                        self._save_dataflows_to_cache(options)
                else:
                    logger.warning(f"OECD API returned status {resp.status_code}")
            except Exception as e:
                logger.error(f"Failed to fetch OECD dataflows: {e}")

        return {
            "indicator": {
                "type": "select",
                "required": True,
                "label": "OECD Dataset (Dataflow)",
                "description": "Select from thousands of OECD datasets",
                "options": options
            }
        }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        def get_clean_value(key: str):
            val = parameters.get(key)
            if isinstance(val, dict): return val.get("value")
            return val

        indicator = get_clean_value("indicator")
        if not indicator:
            raise ValueError("OECD connector requires 'indicator' parameter.")

        if "http" in indicator and "/data/" in indicator:
            indicator = indicator.split("/data/")[1]
            if "?" in indicator: indicator = indicator.split("?")[0]
        
        url = f"data/{indicator}"
        params = {"dimensionAtObservation": "AllDimensions"}
        
        if "startPeriod" in parameters: params["startPeriod"] = parameters["startPeriod"]
        if "endPeriod" in parameters: params["endPeriod"] = parameters["endPeriod"]

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{url}?{query_string}"

    def parse_response(self, response) -> List[Dict[str, Any]]:
        try:
            if hasattr(response, 'json'):
                data = response.json()
            else:
                import json
                data = json.loads(response)
        except:
            return []
        
        root = data.get("data", data)
        datasets = root.get("dataSets", [])
        if not datasets: return []

        structure = None
        if "structures" in root and isinstance(root["structures"], list) and len(root["structures"]) > 0:
            structure = root["structures"][0]
        elif "structure" in root:
            structure = root["structure"]
            
        if not structure: return []
            
        dimensions = structure.get("dimensions", {})
        dimensions_list = []
        
        if isinstance(dimensions, list):
            dimensions_list = dimensions
        elif isinstance(dimensions, dict):
            for key in ["dataset", "series", "observation"]:
                val = dimensions.get(key)
                if val: dimensions_list.extend(val)

        dim_values = {}
        dim_keys = []
        for dim in dimensions_list:
            dim_id = dim.get("id", "unknown")
            dim_keys.append(dim_id)
            dim_values[dim_id] = [val.get("id", val.get("name", "")) for val in dim.get("values", [])]

        normalized = []
        observations = datasets[0].get("observations", {})
        
        for obs_key, obs_value in observations.items():
            record = {}
            parts = str(obs_key).split(":")
            for i, dim_key in enumerate(dim_keys):
                if i < len(parts):
                    try:
                        idx = int(parts[i])
                        if idx < len(dim_values.get(dim_key, [])):
                            record[dim_key.lower()] = dim_values[dim_key][idx]
                    except:
                        record[dim_key.lower()] = parts[i]
            
            val = obs_value[0] if isinstance(obs_value, list) and len(obs_value) > 0 else obs_value
            record["value"] = val
            normalized.append(record)

        return normalized

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        url_path = self.build_url(endpoint, parameters)
        headers = {"Accept": "application/vnd.sdmx.data+json; charset=utf-8; version=2.0, application/json"}
        response = self.make_request(url_path, headers=headers)
        return self.parse_response(response)
