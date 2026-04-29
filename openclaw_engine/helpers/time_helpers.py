"""
Time helpers — timestamp conversion utilities, no trading logic.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional, Any


def utc_now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def unix_ms_to_iso(value: Optional[Any]) -> Optional[str]:
    """Convert Unix milliseconds timestamp to ISO-8601 string."""
    if value is None:
        return None
    try:
        ts = int(value) / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError):
        return None


def parse_iso_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO-8601 timestamp string to datetime (UTC)."""
    if value is None:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def interval_to_seconds(interval: str) -> int:
    """Convert chart interval string to seconds.
    
    Examples: "1m" -> 60, "5m" -> 300, "1h" -> 3600, "1d" -> 86400
    """
    mapping = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
        "3d": 259200,
        "1w": 604800,
    }
    return mapping.get(interval, 60)