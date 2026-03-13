from typing import List, Dict, Any, Optional
import logging
import requests
from connectors.base import BaseConnector
logger = logging.getLogger(__name__)

class OECDConnector(BaseConnector):
    """
    OECD API connector - Teljesen automatizált indikátor-listázással.
    """

    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "oecd_api")
        base_url_fallback = kwargs.pop("base_url_fallback", "https://sdmx.oecd.org/public/rest")
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, **kwargs)

    def get_filter_options(self) -> dict:
            """
            Az összes kért indikátor listája. 
            A build_url automatikusan ki fogja szűrni a lényeget, ha URL-t másolsz be.
            """
            options = [
                {
                    "label": "Agri-environmental indicators: all data", 
                    "value": "OECD.TAD.ARP,DSD_AGRI_ENV@DF_AEI,1.1/.A.TOTAGR_LAND...."
                },
                {
                    "label": "Trust, security and dignity - government at a glance indicators", 
                    "value": "https://sdmx.oecd.org/public/rest/data/OECD.GOV.GIP,DSD_GOV_INT@DF_GOV_TDG_2025,1.1/A.......?startPeriod=2019"
                },
                {
                    "label": "Satisfaction with public services - government at a glance indicators", 
                    "value": "OECD.GOV.GIP,DSD_GOV_INT@DF_GOV_SPS_2025,1.1/A.AUS......"
                },
                {
                    "label": "Adequacy of minimum income benefits", 
                    "value": "OECD.ELS.JAI,DSD_TAXBEN_IA@DF_IA,1.0/..PT_INC_DISP_HH_MEDIAN.S_C0+C_C2...YES+NO.A"
                },
                {
                    "label": "Annual net and gross earnings of minimum wage earners", 
                    "value": "OECD.ELS.JAI,DSD_TAXBEN_IMW@DF_IMW,1.0/.GFE+NFI.XDC+PT_INC_DISP_HH_TP_REF_WG_A.S_C0+C_C2.._Z+NOEARN_UNEMP_WO_CONBEN.YES+NO.YES+NO.FT_ISICCK.MW_DATE.A"
                },
                {
                    "label": "Average annual hours actually worked per worker", 
                    "value": "OECD.ELS.SAE,DSD_HW@DF_AVG_ANN_HRS_WKD,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD........_T...."
                },
                {
                    "label": "Average annual wages", 
                    "value": "OECD.ELS.SAE,DSD_EARNINGS@AV_AN_WAGE,1.0/all"
                },
                {
                    "label": "Trade in Value Added (TiVA) 2025 edition: Origin of value added in final demand", 
                    "value": "OECD.STI.PIE,DSD_TIVA_FDVA@DF_FDVA,1.1/.._T.W._T..A"
                }
            ]

            return {
                "indicator": {
                    "type": "select",
                    "required": True,
                    "label": "OECD Adathalmaz",
                    "description": "Válassz a listából, vagy másolj be egy OECD API URL-t",
                    "options": options
                }
            }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        OECD SDMX REST URL építése tiszta azonosítóból vagy teljes URL-ből.
        """
        indicator = parameters.get("indicator")
        if not indicator:
            raise ValueError("OECD connector requires 'indicator' parameter.")

        # Tisztítás: ha teljes URL-t kapunk, kivágjuk a lényeget
        if "http" in indicator and "/data/" in indicator:
            indicator = indicator.split("/data/")[1]
            if "?" in indicator:
                indicator = indicator.split("?")[0]
        
        # Végpont összeállítása
        url = f"data/{indicator}"
        
        # Minden dimenziót kérünk a helyes parzoláshoz
        params = {"dimensionAtObservation": "AllDimensions"}
        
        # UI paraméterek átadása
        if "startPeriod" in parameters: params["startPeriod"] = parameters["startPeriod"]
        if "endPeriod" in parameters: params["endPeriod"] = parameters["endPeriod"]

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{url}?{query_string}"

    def parse_response(self, response) -> List[Dict[str, Any]]:
        # (A korábbi, már jól működő golyóálló parser kódod marad itt változatlanul...)
        try:
            data = response.json()
        except:
            return []
        
        root = data.get("data", data)
        datasets = root.get("dataSets", [])
        if not datasets: return []

        if "structures" in root and isinstance(root["structures"], list) and len(root["structures"]) > 0:
            structure = root["structures"][0]
        else:
            structure = root.get("structure", {})
            
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
                    idx = int(parts[i]) if parts[i].isdigit() else 0
                    if idx < len(dim_values.get(dim_key, [])):
                        record[dim_key.lower()] = dim_values[dim_key][idx]
            
            val = obs_value[0] if isinstance(obs_value, list) and len(obs_value) > 0 else obs_value
            record["value"] = val
            normalized.append(record)

        return normalized

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        url = self.build_url(endpoint, parameters)
        headers = {"Accept": "application/vnd.sdmx.data+json; charset=utf-8; version=2.0, application/json"}
        response = self.make_request(url, headers=headers)
        return self.parse_response(response)