"""
HTTP helpers — lightweight GET utility, no trading logic.
"""
from typing import Any, Dict, Optional

import requests

TIMEOUT = 20
USER_AGENT = "openclaw-crypto-futures-depth-analysis/1.0"


def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Perform HTTP GET with JSON auto-parse."""
    response = requests.get(url, params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    ctype = response.headers.get("content-type", "")
    if "application/json" in ctype:
        return response.json()
    return response.text