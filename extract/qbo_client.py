import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class QBOClient:
    """HTTP client for QuickBooks Online REST API with pagination, rate limiting, and retry."""

    def __init__(self, oauth_manager, settings):
        self.oauth = oauth_manager
        self.settings = settings
        self._request_timestamps = []

        # Configure session with retry logic
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _rate_limit(self):
        """Enforce QBO rate limit (default 500 requests/minute)."""
        now = time.time()
        self._request_timestamps = [
            t for t in self._request_timestamps if now - t < 60
        ]
        if len(self._request_timestamps) >= self.settings.QBO_RATE_LIMIT_PER_MIN:
            sleep_time = 60 - (now - self._request_timestamps[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self._request_timestamps.append(time.time())

    def _get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.oauth.get_valid_access_token()}",
            "Accept": "application/json",
        }

    def _base_url(self) -> str:
        realm_id = self.oauth.get_realm_id()
        return (
            f"{self.settings.QBO_BASE_URL}/{self.settings.QBO_API_VERSION}"
            f"/company/{realm_id}"
        )

    def query(self, table: str, start_position: int = 1) -> list[dict]:
        """Query a QBO entity table with a single page."""
        self._rate_limit()
        url = f"{self._base_url()}/query"
        query_str = (
            f"SELECT * FROM {table} "
            f"STARTPOSITION {start_position} "
            f"MAXRESULTS {self.settings.QBO_MAX_RESULTS}"
        )
        headers = self._get_headers()
        headers["Content-Type"] = "application/text"

        params = {"minorversion": self.settings.QBO_MINOR_VERSION}

        response = self.session.post(
            url, headers=headers, data=query_str, params=params, timeout=60
        )
        response.raise_for_status()
        return response.json().get("QueryResponse", {}).get(table, [])

    def query_all(self, table: str) -> list[dict]:
        """Paginate through all records for an entity table."""
        all_records = []
        start = 1
        page = 1

        while True:
            logger.debug(f"  {table} page {page} (start={start})")
            batch = self.query(table, start_position=start)
            if not batch:
                break
            all_records.extend(batch)
            if len(batch) < self.settings.QBO_MAX_RESULTS:
                break
            start += self.settings.QBO_MAX_RESULTS
            page += 1

        logger.info(f"  {table}: {len(all_records)} records")
        return all_records

    def get_report(self, report_path: str, params: dict = None) -> dict:
        """Fetch a QBO report endpoint (GET request)."""
        self._rate_limit()
        url = f"{self._base_url()}{report_path}"
        query_params = params or {}
        query_params["minorversion"] = self.settings.QBO_MINOR_VERSION

        response = self.session.get(
            url, headers=self._get_headers(), params=query_params, timeout=60
        )
        response.raise_for_status()
        return response.json()

    def get_company_info(self) -> dict:
        """Fetch company info (special endpoint: GET by ID)."""
        self._rate_limit()
        realm_id = self.oauth.get_realm_id()
        url = f"{self._base_url()}/companyinfo/{realm_id}"
        params = {"minorversion": self.settings.QBO_MINOR_VERSION}

        response = self.session.get(
            url, headers=self._get_headers(), params=params, timeout=30
        )
        response.raise_for_status()
        return response.json().get("CompanyInfo", {})
