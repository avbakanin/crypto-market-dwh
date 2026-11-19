"""Test doubles: an in-memory stand-in for BinanceClient, no network needed."""

from __future__ import annotations

from typing import Any

from ingestion.extract_klines import Kline


class FakeKlinesSource:
    """Serves klines from memory and records the start_time of every request.

    Assumed Binance semantics (consistent with DQ note #13): the API returns candles
    with open_time >= start_time, sorted ascending, at most `limit` of them.
    """

    def __init__(self, klines: list[Kline]) -> None:
        self._klines = sorted(klines, key=lambda k: k[0])
        self.calls: list[int | None] = []

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]:
        self.calls.append(start_time)
        rows = [k for k in self._klines if start_time is None or k[0] >= start_time]
        if end_time is not None:
            rows = [k for k in rows if k[0] <= end_time]
        return rows[:limit]


class FakeBinance:
    """Serves exchangeInfo, 24h tickers and daily klines from memory for select_symbols tests.

    Assumed Binance klines semantics (verified on the live API, DQ notes #13 and #14):
    with start_time -> the first `limit` candles with open_time >= start_time;
    without start_time -> the most recent `limit` candles.
    """

    def __init__(self) -> None:
        self._symbols: list[dict[str, Any]] = []
        self._tickers: list[dict[str, Any]] = []
        self._klines: dict[str, list[Kline]] = {}
        self.kline_calls: list[str] = []  # symbol of every klines request

    def add_symbol(
        self, info: dict[str, Any], quote_volume_24h: float, daily_klines: list[Kline]
    ) -> None:
        symbol = info["symbol"]
        self._symbols.append(info)
        self._tickers.append({"symbol": symbol, "quoteVolume": f"{quote_volume_24h:.8f}"})
        self._klines[symbol] = sorted(daily_klines, key=lambda k: k[0])

    def exchange_info(self) -> dict[str, Any]:
        return {"symbols": self._symbols}

    def ticker_24h(self) -> list[dict[str, Any]]:
        return self._tickers

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]:
        if interval != "1d":
            raise ValueError(f"FakeBinance serves only daily klines, got {interval!r}")
        self.kline_calls.append(symbol)
        rows = self._klines[symbol]  # unknown symbol -> KeyError, like Binance's 400
        if start_time is not None:
            rows = [k for k in rows if k[0] >= start_time]
        if end_time is not None:
            rows = [k for k in rows if k[0] <= end_time]
        return rows[:limit] if start_time is not None else rows[-limit:]
