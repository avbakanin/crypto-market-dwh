"""Test doubles: in-memory stand-ins for BinanceClient and a Snowflake connection and cursor."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from ingestion.clients.binance import BinanceAPIError
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


class FakeCursor:
    """Scripted Snowflake DictCursor: answers each execute() with the next scripted result.

    Records every statement and its parameters; an unscripted statement fails the test.
    """

    def __init__(self, results: list[list[dict[str, Any]]]) -> None:
        self._results = list(results)
        self._rows: list[dict[str, Any]] = []
        self.executed: list[str] = []
        self.params: list[Sequence[Any] | None] = []

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> FakeCursor:
        if not self._results:
            raise AssertionError(f"Unexpected statement: {operation}")
        self.executed.append(operation)
        self.params.append(parameters)
        self._rows = self._results.pop(0)
        return self

    def fetchall(self) -> list[Any]:
        return self._rows

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


class FakeConnection:
    """Stand-in for a SnowflakeConnection in loader.main: hands out one scripted cursor."""

    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self, cursor_class: object = None) -> FakeCursor:
        return self._cursor

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


class FakeFutures:
    """Serves the USD-M futures endpoints from memory and records every request.

    Live semantics (verified, Week 2 Step 2): funding_rate with start_time -> the first `limit`
    records with fundingTime >= start_time (inclusive), ascending; start_time <= 0 is refused,
    as by the real client; without start_time -> the most recent `limit`. funding_info lists
    the contracts given an interval. futures_data (open interest = "openInterestHist", any
    other /futures/data/ endpoint given in `stats`) returns the LAST `limit` points with
    start_time <= timestamp <= end_time; a start_time or end_time before `served_from` is
    HTTP 400 -1130, as live for times older than about a month. An unknown contract or
    endpoint raises KeyError.
    """

    def __init__(
        self,
        funding: dict[str, list[dict[str, Any]]] | None = None,
        intervals: dict[str, int] | None = None,
        open_interest: dict[str, list[dict[str, Any]]] | None = None,
        served_from: int | None = None,
        stats: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    ) -> None:
        self._funding = {
            s: sorted(rows, key=lambda r: r["fundingTime"]) for s, rows in (funding or {}).items()
        }
        self._intervals = intervals or {}
        by_endpoint = {"openInterestHist": open_interest or {}, **(stats or {})}
        self._stats = {
            endpoint: {s: sorted(rows, key=lambda r: r["timestamp"]) for s, rows in series.items()}
            for endpoint, series in by_endpoint.items()
        }
        self._served_from = served_from
        self.info_calls = 0
        self.funding_calls: list[tuple[str, int | None]] = []  # (symbol, start_time)
        # (endpoint, symbol, start, end) of every futures_data request
        self.stats_calls: list[tuple[str, str, int | None, int | None]] = []

    def __enter__(self) -> FakeFutures:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def funding_info(self) -> list[dict[str, Any]]:
        self.info_calls += 1
        return [
            {
                "symbol": symbol,
                "adjustedFundingRateCap": "0.02000000",
                "adjustedFundingRateFloor": "-0.02000000",
                "fundingIntervalHours": hours,
                "disclaimer": False,
                "updateTime": 0,
            }
            for symbol, hours in self._intervals.items()
        ]

    def funding_rate(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if start_time is not None and start_time <= 0:
            raise ValueError(f"start_time must be positive, got {start_time}")
        self.funding_calls.append((symbol, start_time))
        rows = self._funding[symbol]
        if end_time is not None:
            rows = [r for r in rows if r["fundingTime"] <= end_time]
        if start_time is None:
            return rows[-limit:]
        return [r for r in rows if r["fundingTime"] >= start_time][:limit]

    def futures_data(
        self,
        endpoint: str,
        symbol: str,
        period: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if period != "1h":
            raise ValueError(f"FakeFutures serves 1h statistics only, got {period!r}")
        self.stats_calls.append((endpoint, symbol, start_time, end_time))
        for name, value in (("startTime", start_time), ("endTime", end_time)):
            if value is not None and self._served_from is not None and value < self._served_from:
                body = '{"msg":"parameter ' + repr(name) + ' is invalid.","code":-1130}'
                raise BinanceAPIError(400, f"/futures/data/{endpoint}", body)
        rows = self._stats[endpoint][symbol]
        if start_time is not None:
            rows = [r for r in rows if r["timestamp"] >= start_time]
        if end_time is not None:
            rows = [r for r in rows if r["timestamp"] <= end_time]
        return rows[-limit:]


@dataclass(frozen=True)
class Column:
    """The fields of the connector's ResultMetadata that the export reads."""

    name: str
    type_code: int
    precision: int | None = None
    scale: int | None = None


class FakeArrowCursor:
    """Stand-in for a SnowflakeCursor serving SELECTs as Arrow tables, as fetch_arrow_all does.

    `results` maps a relation to its result metadata and the Arrow table the connector would
    hand over (upper-case names, value-dependent types). Live semantics (probe 2026-11-24):
    an empty result is None unless force_return_table. The context query answers with the
    session's role and available roles. A relation in `fail_on` raises, as a lost connection.
    """

    def __init__(
        self,
        results: dict[str, tuple[list[Column], pa.Table]],
        role: str = "CRYPTO_REPORTER",
        available_roles: Sequence[str] = ("CRYPTO_REPORTER", "PUBLIC"),
        fail_on: str | None = None,
    ) -> None:
        self._results = results
        self._context = (role, json.dumps(list(available_roles)))
        self._fail_on = fail_on
        self._current: tuple[list[Column], pa.Table] | None = None
        self._row: tuple[str, str] | None = None
        self.executed: list[str] = []
        self.fetch_kwargs: list[dict[str, Any]] = []

    @property
    def description(self) -> list[Column] | None:
        return self._current[0] if self._current else None

    @property
    def sfqid(self) -> str | None:
        return f"query-{len(self.executed)}" if self.executed else None

    def execute(self, command: str, /) -> FakeArrowCursor:
        self.executed.append(command)
        if command.startswith("SELECT CURRENT_ROLE()"):
            self._row, self._current = self._context, None
            return self
        relation = command.split(" FROM ", 1)[1].split()[0]
        if relation == self._fail_on:
            raise RuntimeError(f"connection lost while reading {relation}")
        self._row, self._current = None, self._results[relation]
        return self

    def fetchone(self) -> tuple[str, str] | None:
        return self._row

    def fetch_arrow_all(
        self, force_return_table: bool = False, force_microsecond_precision: bool | None = None
    ) -> pa.Table | None:
        self.fetch_kwargs.append(
            {
                "force_return_table": force_return_table,
                "force_microsecond_precision": force_microsecond_precision,
            }
        )
        if self._current is None:
            raise AssertionError("fetch_arrow_all without a SELECT")
        table = self._current[1]
        if table.num_rows == 0 and not force_return_table:
            return None
        return table

    def __enter__(self) -> FakeArrowCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


class FakeArrowConnection:
    """Stand-in for the reporter's SnowflakeConnection: hands out one FakeArrowCursor."""

    def __init__(self, cursor: FakeArrowCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeArrowCursor:
        return self._cursor

    def __enter__(self) -> FakeArrowConnection:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None
