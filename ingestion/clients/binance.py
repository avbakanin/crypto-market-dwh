"""Thin Binance REST client: shared Session, timeouts, retries on transient errors."""

from __future__ import annotations

import time
from types import TracebackType
from typing import Any

import requests

BASE_URL = "https://api.binance.com"
DEFAULT_TIMEOUT_S = 10
DEFAULT_MAX_RETRIES = 5
BACKOFF_BASE_S = 1.0

# Transient: worth retrying. 418 (IP ban) is deliberately NOT here.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


class BinanceAPIError(RuntimeError):
    """Non-retryable (or retries exhausted) HTTP error with Binance response body."""

    def __init__(self, status_code: int, path: str, body: str) -> None:
        super().__init__(f"HTTP {status_code} on {path}: {body}")
        self.status_code = status_code
        self.path = path
        self.body = body


class BinanceClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self.base_url = base_url
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.session = requests.Session()

    # --- context manager: closes the Session ---
    def __enter__(self) -> BinanceClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self.session.close()

    # --- core ---
    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET with retries on 429/5xx and network errors; raise BinanceAPIError otherwise."""
        url = f"{self.base_url}{path}"

        for attempt in range(1, self.max_retries + 1):
            is_last = attempt == self.max_retries
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_s)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if is_last:
                    raise
                wait = self._backoff(attempt)
                print(f"  {type(exc).__name__} on {path}, retry {attempt}/{self.max_retries} in {wait:.0f}s")
                time.sleep(wait)
                continue

            if response.ok:
                return response.json()

            if response.status_code in RETRY_STATUS_CODES and not is_last:
                wait = self._retry_after(response) or self._backoff(attempt)
                print(f"  HTTP {response.status_code} on {path}, retry {attempt}/{self.max_retries} in {wait:.0f}s")
                time.sleep(wait)
                continue

            raise BinanceAPIError(response.status_code, path, response.text)

        raise AssertionError("unreachable")  # keeps type checker happy

    @staticmethod
    def _backoff(attempt: int) -> float:
        return BACKOFF_BASE_S * 2 ** (attempt - 1)  # 1, 2, 4, 8 ...

    @staticmethod
    def _retry_after(response: requests.Response) -> float | None:
        header = response.headers.get("Retry-After")
        try:
            return float(header) if header else None
        except ValueError:
            return None

    # --- endpoints ---
    def exchange_info(self) -> dict[str, Any]:
        return self.get("/api/v3/exchangeInfo")

    def ticker_24h(self) -> list[dict[str, Any]]:
        return self.get("/api/v3/ticker/24hr")

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[list[Any]]:
        """Raw klines. start_time/end_time are Unix ms UTC (DQ note #4)."""
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self.get("/api/v3/klines", params)