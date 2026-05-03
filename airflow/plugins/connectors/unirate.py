from typing import List, Dict, Any, Optional
import requests
import logging
import os
import json
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class UniRateConnector(BaseConnector):
    """
    UniRate API connector for exchange rates and currency conversion.
    Optimized with local caching for currency lists.
    """

    _cache_file = os.path.join(os.path.dirname(__file__), "unirate_currencies.json")

    def __init__(self, **kwargs):
        conn_id = kwargs.pop("conn_id", "unirate_api")
        self.api_key = os.getenv("UNIRATE_API_KEY", "dLkIJ6VpPVY9c1HCjlNrBTXycfiJNf5lA4QLBdwlGIGRgTR3XsboZ3BW9gjEi9T4")
        base_url_fallback = kwargs.pop("base_url_fallback", "https://api.unirateapi.com/api")
        super().__init__(conn_id=conn_id, base_url_fallback=base_url_fallback, use_hook=True, **kwargs)

    def _get_cached_currencies(self) -> Optional[List[Dict[str, str]]]:
        if os.path.exists(self._cache_file):
            try:
                with open(self._cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data and isinstance(data, list):
                        return data
            except Exception as e:
                logger.error(f"Error reading UniRate cache: {e}")
        return None

    def _save_currencies_to_cache(self, currencies: List[Dict[str, str]]):
        try:
            with open(self._cache_file, "w", encoding="utf-8") as f:
                json.dump(currencies, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error writing UniRate cache: {e}")

    def get_filter_options(self, current_params: Dict[str, Any] = None) -> dict:
        currency_options = self._get_cached_currencies()
        
        if not currency_options:
            currency_options = [
                {"label": "USD - US Dollar", "value": "USD"},
                {"label": "EUR - Euro", "value": "EUR"},
                {"label": "GBP - British Pound", "value": "GBP"},
                {"label": "JPY - Japanese Yen", "value": "JPY"},
                {"label": "HUF - Hungarian Forint", "value": "HUF"}
            ]
            try:
                url = f"{self.base_url_fallback}/currencies?api_key={self.api_key}"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    fetched_options = []
                    if isinstance(data, list):
                        for c in data:
                            code = c.get("code") or c.get("symbol")
                            name = c.get("name")
                            if code: fetched_options.append({"label": f"{code} - {name}" if name else code, "value": code})
                    elif isinstance(data, dict):
                        source = data.get("currencies", data) if "currencies" in data else data
                        for code, name in source.items():
                             fetched_options.append({"label": f"{code} - {name}" if isinstance(name, str) else str(code), "value": str(code)})
                    if fetched_options:
                        currency_options = sorted(fetched_options, key=lambda x: x["label"])
                        self._save_currencies_to_cache(currency_options)
            except Exception as e:
                logger.error(f"Error fetching UniRate currencies: {e}")

        return {
            "endpoint_type": {
                "type": "select",
                "label": "Data Type",
                "required": True,
                "options": [
                    {"label": "Current Rates (Base focus)", "value": "rates"},
                    {"label": "Currency Conversion (Specific pair)", "value": "convert"}
                ],
                "default": "rates"
            },
            "from_currency": {
                "type": "select",
                "label": "From / Base Currency",
                "options": currency_options,
                "default": "USD"
            },
            "to_currency": {
                "type": "select",
                "label": "To Currency (Only for Convert)",
                "options": currency_options,
                "default": "HUF",
                "required": False
            },
            "amount": {
                "type": "text",
                "label": "Amount (Only for Convert)",
                "default": "1",
                "required": False
            }
        }

    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        def get_clean_value(key: str, default: str = ""):
            val = parameters.get(key, default)
            if isinstance(val, dict): return val.get("value", default)
            return val

        endpoint_type = get_clean_value("endpoint_type", "rates")
        from_curr = get_clean_value("from_currency", "USD")
        
        query_params = [f"api_key={self.api_key}", f"from={from_curr}"]

        if endpoint_type == "convert":
            to_curr = get_clean_value("to_currency", "HUF")
            amount = get_clean_value("amount", "1")
            query_params.extend([f"to={to_curr}", f"amount={amount}"])

        return f"{endpoint_type}?" + "&".join(query_params)

    def parse_response(self, response) -> List[Dict[str, Any]]:
        try:
            data = response.json() if hasattr(response, 'json') else json.loads(response)
            if isinstance(data, dict):
                # Handle /rates response
                if "rates" in data:
                    rates = data["rates"]
                    base = data.get("base") or data.get("from")
                    date = data.get("date") or data.get("timestamp")
                    return [{"base": base, "currency": c, "rate": r, "date": date} for c, r in rates.items()]
                # Handle /convert response
                if "result" in data:
                    return [{
                        "from": data.get("from"),
                        "to": data.get("to"),
                        "amount": data.get("amount"),
                        "result": data.get("result"),
                        "rate": data.get("rate"),
                        "timestamp": data.get("timestamp") or data.get("date")
                    }]
                return [data]
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"Error parsing UniRate response: {e}")
            return []

    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        url_path = self.build_url(endpoint, parameters)
        try:
            response = self.make_request(url_path)
            return self.parse_response(response)
        except Exception as e:
            logger.error(f"UniRate fetch error: {e}")
            raise
