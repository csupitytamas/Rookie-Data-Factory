from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import logging
import requests

try:
    from airflow.providers.http.hooks.http import HttpHook
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    def __init__(
        self,
        conn_id: str,
        base_url_fallback: str = None,
        **kwargs
    ):
        self.conn_id = conn_id
        self.base_url_fallback = base_url_fallback
        
        if AIRFLOW_AVAILABLE:
            self.hook = HttpHook(http_conn_id=conn_id, method='GET')
        else:
            self.hook = None
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "ETL-App/1.0",
                "Accept": "application/json"
            })

    def make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        # 1. AIRFLOW MÓD (ETL futás)
        if self.hook:
            logger.info(f"[{self.conn_id}] Airflow Hook kérés: /{endpoint}")
            return self.hook.run(endpoint=endpoint, data=params, headers=headers, extra_options=kwargs)
            
        # 2. FASTAPI BACKEND MÓD (UI szűrők lekérése)
        else:
            url = f"{self.base_url_fallback}/{endpoint}" if self.base_url_fallback else endpoint
            logger.info(f"[{self.conn_id}] FastAPI Fallback kérés: {url}")
            
            request_headers = self.session.headers.copy()
            if headers:
                request_headers.update(headers)
                
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=request_headers,
                timeout=30,
                **kwargs
            )
            response.raise_for_status()
            return response

    @abstractmethod
    def get_filter_options(self) -> dict:
        return {}

    @abstractmethod
    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        pass

    @abstractmethod
    def parse_response(self, response) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def fetch(self, endpoint: str, parameters: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        pass

    def normalize_data(self, raw_data: List[Dict[str, Any]], field_mappings: Optional[List[Dict[str, str]]] = None) -> List[Dict[str, Any]]:
        if not field_mappings: return raw_data
        normalized = []
        for item in raw_data:
            record = {}
            for field in field_mappings:
                field_name = field.get("name")
                field_path = field.get("path", field_name)
                record[field_name] = self._extract_from_path(item, field_path)
            normalized.append(record)
        return normalized

    def _extract_from_path(self, data: Dict[str, Any], path: str) -> Any:
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict): value = value.get(key)
            else: return None
            if value is None: return None
        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.hook and hasattr(self, 'session'):
            self.session.close()