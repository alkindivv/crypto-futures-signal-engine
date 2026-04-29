"""
client.py — HTTP client for Binance REST API.

Extracted from public_market_data.py:
- PublicMarketData.get() method (via ProviderClient)
- binance_get() convenience functions

Preserves:
- requests.get with TIMEOUT and USER_AGENT
- Exception handling (returns empty dict on failure)
- Concurrent fetch patterns via asyncio.gather
"""
from typing import Any, Dict, Optional

import requests

TIMEOUT = 20
USER_AGENT = "openclaw-crypto-futures-depth-analysis/1.0"
BINANCE_REST = "https://fapi.binance.com"


class BinanceClient:
    """Lightweight Binance REST client.

    Wraps requests.get with consistent timeout and headers.
    Used by PublicMarketData.get() — identical behavior.
    """

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def get(self, base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET request with timeout and error handling."""
        try:
            url = base + path
            response = self.session.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception:
            return {}

    def close(self) -> None:
        self.session.close()


def binance_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Standalone binance GET request using default BINANCE_REST."""
    try:
        url = f"https://fapi.binance.com{path}"
        response = requests.get(url, params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}