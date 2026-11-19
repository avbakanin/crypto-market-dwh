"""Tests for select_symbols whitelist rules over a fake exchange, no network.

Every test market is otherwise perfect (trading, spot, USDT, long history, liquid, not pegged),
so the only rule that can drop it is the one under test.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from ingestion import select_symbols as sel
from ingestion.extract_klines import DAY_MS, Kline
from tests.factories import make_daily_klines, make_symbol_info
from tests.fakes import FakeBinance


def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)  # midnight: today's daily candle opens at NOW
NOW_MS = to_ms(NOW)
CUTOFF_MS = NOW_MS - sel.MIN_HISTORY_DAYS * DAY_MS  # 2025-01-01 00:00 UTC
LISTED_LONG_AGO_MS = to_ms(datetime(2020, 1, 1, tzinfo=timezone.utc))
LIQUID = 2.0 * sel.MIN_AVG_QUOTE_VOLUME_USDT  # comfortably above both volume thresholds


@pytest.fixture(autouse=True)
def no_min_selected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rule tests use 1-3 markets; the >= 20 guard has its own tests below."""
    monkeypatch.setattr(sel, "MIN_SELECTED", 0)


def daily(
    listed_ms: int = LISTED_LONG_AGO_MS,
    close: str = "100.00000000",
    avg_volume: float = LIQUID,
) -> list[Kline]:
    """Daily candles from listing up to today's still-open candle (opened at NOW)."""
    return make_daily_klines(listed_ms, NOW_MS, close=close, quote_volume=f"{avg_volume:.8f}")


def add_market(
    fake: FakeBinance,
    info: dict[str, Any],
    klines: list[Kline] | None = None,
    volume_24h: float = LIQUID,
) -> None:
    fake.add_symbol(info, volume_24h, daily() if klines is None else klines)


def selected(fake: FakeBinance) -> list[str]:
    return [row["symbol"] for row in sel.select_symbols(fake, now=NOW)]


def test_liquid_symbol_with_long_history_is_selected() -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("BTC"))

    assert sel.select_symbols(fake, now=NOW) == [
        {
            "symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "avg_quote_volume_30d_usdt": 20_000_000,
            "quote_volume_24h_usdt": 20_000_000,
            "first_candle_date": "2020-01-01",
            "selected_at_utc": "2026-01-01T00:00:00+00:00",
        }
    ]


@pytest.mark.parametrize(
    "info",
    [
        make_symbol_info("ETH", status="BREAK"),
        make_symbol_info("ETH", quote="BTC"),
        make_symbol_info("ETH", spot=False),
        make_symbol_info("USDC"),  # stablecoin
        make_symbol_info("WBTC"),  # wrapped duplicate
    ],
    ids=["not-trading", "quote-not-usdt", "not-spot", "stablecoin", "wrapped"],
)
def test_ineligible_listing_is_dropped(info: dict[str, Any]) -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("BTC"))
    add_market(fake, info)

    assert selected(fake) == ["BTCUSDT"]


@pytest.mark.parametrize(
    ("volume_24h", "is_selected"),
    [
        (sel.MIN_24H_VOLUME_PREFILTER_USDT, True),
        (sel.MIN_24H_VOLUME_PREFILTER_USDT - 0.01, False),
    ],
    ids=["exactly-threshold", "just-below"],
)
def test_24h_prefilter_boundary(volume_24h: float, is_selected: bool) -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("ETH"), volume_24h=volume_24h)

    assert (selected(fake) == ["ETHUSDT"]) is is_selected


def test_prefiltered_symbol_costs_no_kline_requests() -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("BTC"))
    add_market(fake, make_symbol_info("ETH"), volume_24h=0.0)

    selected(fake)

    assert "ETHUSDT" not in fake.kline_calls
    assert fake.kline_calls == ["BTCUSDT", "BTCUSDT"]  # first candle + 30d stats


@pytest.mark.parametrize(
    ("listed_ms", "is_selected"),
    [(CUTOFF_MS, True), (CUTOFF_MS + DAY_MS, False)],
    ids=["exactly-365-days", "364-days"],
)
def test_history_boundary(listed_ms: int, is_selected: bool) -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("ETH"), klines=daily(listed_ms=listed_ms))

    assert (selected(fake) == ["ETHUSDT"]) is is_selected


@pytest.mark.parametrize(
    ("min_close", "max_close", "is_selected"),
    [
        ("0.98000000", "1.02000000", False),  # band edges are still "pegged"
        ("0.97990000", "1.00000000", True),
        ("1.00000000", "1.02010000", True),
    ],
    ids=["inside-band-edges", "dips-below", "spikes-above"],
)
def test_usd_peg_band(min_close: str, max_close: str, is_selected: bool) -> None:
    klines = daily(close="1.00000000")
    klines[-2][4] = min_close  # yesterday (index 4 = close)
    klines[-3][4] = max_close  # the day before
    fake = FakeBinance()
    add_market(fake, make_symbol_info("XYZ"), klines=klines)

    assert (selected(fake) == ["XYZUSDT"]) is is_selected


@pytest.mark.parametrize(
    ("avg_volume", "is_selected"),
    [
        (sel.MIN_AVG_QUOTE_VOLUME_USDT, True),
        (sel.MIN_AVG_QUOTE_VOLUME_USDT - 0.01, False),
    ],
    ids=["exactly-threshold", "just-below"],
)
def test_avg_volume_boundary(avg_volume: float, is_selected: bool) -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("ETH"), klines=daily(avg_volume=avg_volume))

    assert (selected(fake) == ["ETHUSDT"]) is is_selected


def test_todays_open_candle_is_excluded_from_average() -> None:
    klines = daily(avg_volume=sel.MIN_AVG_QUOTE_VOLUME_USDT)
    klines[-1][7] = "0.00000000"  # today's candle right after midnight: no volume yet
    fake = FakeBinance()
    add_market(fake, make_symbol_info("ETH"), klines=klines)

    assert selected(fake) == ["ETHUSDT"]


def test_average_window_is_exactly_last_30_closed_days() -> None:
    window = sel.VOLUME_WINDOW_DAYS
    threshold = sel.MIN_AVG_QUOTE_VOLUME_USDT
    klines = daily(avg_volume=0.0)
    klines[-1 - window][7] = f"{threshold * window:.8f}"  # oldest day inside the window
    fake = FakeBinance()
    add_market(fake, make_symbol_info("ETH"), klines=klines)

    # 29-day window misses that day (avg 0); 31-day window dilutes it (avg < threshold)
    assert selected(fake) == ["ETHUSDT"]


def test_top_n_by_30d_average_volume_descending(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sel, "TOP_N", 2)
    fake = FakeBinance()
    # 24h volumes rank in the opposite order: ranking must use the 30d average (DQ note #10)
    add_market(fake, make_symbol_info("AAA"), daily(avg_volume=30_000_000), volume_24h=20_000_000)
    add_market(fake, make_symbol_info("BBB"), daily(avg_volume=20_000_000), volume_24h=40_000_000)
    add_market(fake, make_symbol_info("CCC"), daily(avg_volume=40_000_000), volume_24h=10_000_000)

    assert selected(fake) == ["CCCUSDT", "AAAUSDT"]


def test_too_few_selected_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sel, "MIN_SELECTED", 2)
    fake = FakeBinance()
    add_market(fake, make_symbol_info("BTC"))

    with pytest.raises(RuntimeError, match="Only 1 symbols"):
        selected(fake)


def test_exactly_min_selected_is_enough(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sel, "MIN_SELECTED", 2)
    fake = FakeBinance()
    add_market(fake, make_symbol_info("BTC"))
    add_market(fake, make_symbol_info("ETH"))

    assert selected(fake) == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.xfail(
    strict=True,
    reason="Known gap (DQ note #9): no rule for tokenized stocks; only history filters them.",
)
def test_tokenized_stock_with_long_history_is_dropped() -> None:
    fake = FakeBinance()
    add_market(fake, make_symbol_info("NVDAB"), klines=daily(close="180.00000000"))

    assert "NVDABUSDT" not in selected(fake)
