"""Test doubles: an in-memory stand-in for BinanceClient, no network needed."""

from __future__ import annotations

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
