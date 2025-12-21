from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Alaposztály minden API connector számára.
    Minden connector ugyanazt a fetch() interfészt valósítja meg,
    de a forrás-specifikus lépéseket (URL-építés, auth, lapozás, formátum-parzolás) maga intézi.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        **kwargs
    ):
        """
        Inicializálás.
        
        Args:
            base_url: Az API alap URL-je
            timeout: Request timeout másodpercben
            max_retries: Maximum újrapróbálkozások száma
            **kwargs: További connector-specifikus paraméterek
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self._setup_session()

    def _setup_session(self):
        """Session beállítása (headers, auth, stb.) - override-olható"""
        self.session.headers.update({
            "User-Agent": "ETL-App/1.0",
            "Accept": "application/json"
        })

    def make_request(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        HTTP kérés végrehajtása retry logikával.
        
        Args:
            url: A teljes URL
            method: HTTP metódus (GET, POST, stb.)
            params: Query paraméterek
            headers: Egyedi headers
            **kwargs: További requests paraméterek
            
        Returns:
            requests.Response objektum
        """
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=request_headers,
                    timeout=self.timeout,
                    **kwargs
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff

        raise Exception(f"Request failed after {self.max_retries} attempts: {last_exception}")
    @abstractmethod
    def get_filter_options(self) -> dict:
        return {}

    @abstractmethod
    def build_url(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        URL építése endpoint és paraméterek alapján.
        
        Args:
            endpoint: Az endpoint (pl. "indicator", "country", stb.)
            parameters: A paraméterek dict-je
            
        Returns:
            A teljes URL string
        """
        pass

    @abstractmethod
    def parse_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        """
        Válasz parzolása és normalizálása standard formátumba.
        
        Args:
            response: A requests.Response objektum
            
        Returns:
            Lista dict-ekkel, minden dict egy rekordot reprezentál
        """
        pass

    @abstractmethod
    def fetch(
        self,
        endpoint: str,
        parameters: Dict[str, Any],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Fő fetch metódus - ezt hívja meg a pipeline.
        
        Args:
            endpoint: Az endpoint azonosító
            parameters: A paraméterek (pl. {"indicator": "SP.POP.TOTL", "country": "USA"})
            **kwargs: További opcionális paraméterek
            
        Returns:
            Normalizált lista dict-ekkel: [{ "country": "...", "indicator": "...", "value": ..., "year": ... }]
        """
        pass

    def normalize_data(
        self,
        raw_data: List[Dict[str, Any]],
        field_mappings: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Adatok normalizálása field_mappings alapján (opcionális).
        
        Args:
            raw_data: A nyers adatok
            field_mappings: Mezőleképezések (pl. [{"name": "country", "path": "country.value"}])
            
        Returns:
            Normalizált adatok
        """
        if not field_mappings:
            return raw_data

        normalized = []
        for item in raw_data:
            record = {}
            for field in field_mappings:
                field_name = field.get("name")
                field_path = field.get("path", field_name)
                value = self._extract_from_path(item, field_path)
                record[field_name] = value
            normalized.append(record)

        return normalized

    def _extract_from_path(self, data: Dict[str, Any], path: str) -> Any:
        """
        Érték kinyerése nested dict-ből path alapján (pl. "country.value").
        
        Args:
            data: A dict
            path: A path (pl. "country.value" vagy "value")
            
        Returns:
            Az érték vagy None
        """
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
            if value is None:
                return None
        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

