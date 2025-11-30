"""Utility helpers for querying Loki with explicit time ranges.

Time ranges are always computed as Unix nanoseconds between the current UTC
clock reading and a caller-provided number of hours in the past. No Loki
"now" placeholder is used.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional


def _to_unix_ns(dt: datetime) -> int:
    """Convert a timezone-aware ``datetime`` to a Unix timestamp in nanoseconds."""
    return int(dt.timestamp() * 1_000_000_000)


def calculate_range(hours_ago: float) -> tuple[int, int]:
    """Return ``(start_ns, end_ns)`` covering ``hours_ago`` up to now in UTC.

    Args:
        hours_ago: Number of hours to look back from the current UTC time. Must
            be non-negative.

    Raises:
        ValueError: If ``hours_ago`` is negative.
    """
    if hours_ago < 0:
        raise ValueError("hours_ago must be non-negative")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours_ago)
    return _to_unix_ns(start), _to_unix_ns(now)


@dataclass
class LokiAuth:
    """Authentication details for Loki."""

    basic_auth: Optional[tuple[str, str]] = None
    bearer_token: Optional[str] = None

    def headers(self) -> Dict[str, str]:
        header: Dict[str, str] = {}
        if self.basic_auth and self.bearer_token:
            raise ValueError("Use either basic_auth or bearer_token, not both.")

        if self.basic_auth:
            user, password = self.basic_auth
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            header["Authorization"] = f"Basic {token}"
        elif self.bearer_token:
            header["Authorization"] = f"Bearer {self.bearer_token}"
        return header


class LokiClient:
    """Minimal Loki HTTP client focused on range queries."""

    def __init__(
        self,
        base_url: str,
        auth: Optional[LokiAuth] = None,
        *,
        timeout: float = 10.0,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self.auth = auth or LokiAuth()
        self.timeout = timeout

    def query_range(
        self,
        query: str,
        hours_ago: float,
        *,
        step_seconds: float = 60.0,
        limit: Optional[int] = None,
        direction: str = "BACKWARD",
    ) -> List[Dict[str, object]]:
        """Execute a range query and return normalized Loki log records.

        Args:
            query: LogQL query string.
            hours_ago: How many hours before now to start the range from.
            step_seconds: Interval used by Loki to evaluate the query.
            limit: Optional maximum number of entries to return.
            direction: Either ``"BACKWARD"`` (default) or ``"FORWARD"``.

        Returns:
            A list of dictionaries, each containing ``timestamp`` (as the Loki
            nanosecond timestamp string), ``labels`` (the stream labels), and
            ``record`` (the log line payload).
        """
        start_ns, end_ns = calculate_range(hours_ago)
        params = {
            "query": query,
            "start": start_ns,
            "end": end_ns,
            "step": f"{step_seconds:g}s",
            "direction": direction,
        }
        if limit is not None:
            params["limit"] = str(limit)

        endpoint = f"{self.base_url}/loki/api/v1/query_range"
        url = f"{endpoint}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"Accept": "application/json", **self.auth.headers()})

        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = response.read().decode()
        except urllib.error.HTTPError as exc:  # pragma: no cover - handled in integration
            raise RuntimeError(f"Loki query failed with HTTP {exc.code}: {exc.read().decode()}") from exc
        except urllib.error.URLError as exc:  # pragma: no cover - handled in integration
            raise RuntimeError(f"Failed to reach Loki: {exc.reason}") from exc

        data = json.loads(payload)
        return self._parse_streams(data)

    @staticmethod
    def _parse_streams(data: Dict[str, object]) -> List[Dict[str, object]]:
        """Flatten Loki stream results into a list of record dictionaries."""
        result = []
        data_block = data.get("data", {}) if isinstance(data, dict) else {}
        streams: Iterable[Dict[str, object]] = data_block.get("result", []) if isinstance(data_block, dict) else []

        for stream in streams:
            labels = stream.get("stream", {}) if isinstance(stream, dict) else {}
            values = stream.get("values", []) if isinstance(stream, dict) else []
            for entry in values:
                if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                    continue
                timestamp, record = entry
                result.append({
                    "timestamp": timestamp,
                    "labels": labels,
                    "record": record,
                })
        return result


__all__ = [
    "LokiAuth",
    "LokiClient",
    "calculate_range",
]
