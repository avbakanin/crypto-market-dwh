"""Rules of the spot -> USD-M perpetual mapping: each contract breaks exactly one rule."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from ingestion.map_perpetuals import map_perpetuals
from tests.factories import make_contract

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def info(*contracts: dict[str, Any]) -> dict[str, Any]:
    return {"symbols": list(contracts)}


def mapped(spot: list[tuple[str, str]], *contracts: dict[str, Any]) -> dict[str, str]:
    rows, _ = map_perpetuals(spot, info(*contracts), NOW)
    return {r["spot_symbol"]: r["futures_symbol"] for r in rows}


def test_same_symbol_contract_is_mapped() -> None:
    rows, unmapped = map_perpetuals([("BTCUSDT", "BTC")], info(make_contract("BTC")), NOW)
    assert rows == [
        {
            "spot_symbol": "BTCUSDT",
            "futures_symbol": "BTCUSDT",
            "contract_multiplier": 1,
            "mapped_at_utc": "2026-01-01T00:00:00+00:00",
        }
    ]
    assert unmapped == []


@pytest.mark.parametrize(
    ("contract_base", "multiplier"),
    [("1000PEPE", 1_000), ("1000000PEPE", 1_000_000), ("1MPEPE", 1_000_000)],
)
def test_contract_quoted_per_n_coins_keeps_its_multiplier(
    contract_base: str, multiplier: int
) -> None:
    rows, _ = map_perpetuals([("PEPEUSDT", "PEPE")], info(make_contract(contract_base)), NOW)
    assert (rows[0]["futures_symbol"], rows[0]["contract_multiplier"]) == (
        f"{contract_base}USDT",
        multiplier,
    )


def test_lookalike_symbols_are_not_matched() -> None:
    # Substrings of the symbol: a dominance index and another coin.
    spot = [("BTCUSDT", "BTC"), ("PUMPUSDT", "PUMP")]
    rows, unmapped = map_perpetuals(
        spot, info(make_contract("BTCDOM"), make_contract("PUMPBTC"), make_contract("BTC")), NOW
    )
    assert [r["futures_symbol"] for r in rows] == ["BTCUSDT"]
    assert unmapped == ["PUMPUSDT"]


@pytest.mark.parametrize(
    "contract",
    [
        make_contract("BTC", symbol="BTCUSDT_261225", contract_type="CURRENT_QUARTER"),
        make_contract("BTC", contract_type="TRADIFI_PERPETUAL"),
        make_contract("BTC", status="SETTLING"),
        make_contract("BTC", quote="USDC"),
    ],
    ids=["quarterly", "tradifi", "settling", "usdc-quoted"],
)
def test_contract_that_is_not_a_live_usdt_perpetual_is_ignored(contract: dict[str, Any]) -> None:
    rows, unmapped = map_perpetuals(
        [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")], info(contract, make_contract("ETH")), NOW
    )
    assert [r["spot_symbol"] for r in rows] == ["ETHUSDT"]
    assert unmapped == ["BTCUSDT"]


def test_two_contracts_for_one_coin_stop_the_mapping() -> None:
    with pytest.raises(ValueError, match=r"PEPEUSDT: several perpetuals \['1000PEPEUSDT', "):
        map_perpetuals(
            [("PEPEUSDT", "PEPE")], info(make_contract("PEPE"), make_contract("1000PEPE")), NOW
        )


def test_rows_follow_whitelist_order() -> None:
    spot = [("SOLUSDT", "SOL"), ("BTCUSDT", "BTC")]
    assert list(mapped(spot, make_contract("BTC"), make_contract("SOL"))) == ["SOLUSDT", "BTCUSDT"]


def test_no_contract_for_any_symbol_raises() -> None:
    with pytest.raises(RuntimeError, match="No whitelisted symbol"):
        map_perpetuals([("BTCUSDT", "BTC")], info(make_contract("ETH")), NOW)


def test_naive_now_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        map_perpetuals([("BTCUSDT", "BTC")], info(make_contract("BTC")), datetime(2026, 1, 1))


def test_non_utc_now_is_normalized_to_utc() -> None:
    plus3 = NOW.astimezone(timezone(timedelta(hours=3)))
    rows, _ = map_perpetuals([("BTCUSDT", "BTC")], info(make_contract("BTC")), plus3)
    assert rows[0]["mapped_at_utc"] == "2026-01-01T00:00:00+00:00"
