"""Rules of the spot -> USD-M perpetual mapping: each contract breaks exactly one rule."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from ingestion.map_perpetuals import map_perpetuals
from tests.factories import make_contract, make_premium_index

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def run(
    spot: list[tuple[str, str]],
    *contracts: dict[str, Any],
    rates: dict[str, str] | None = None,
    now: datetime = NOW,
) -> tuple[list[dict[str, Any]], list[str]]:
    """map_perpetuals on these contracts; every contract is in premiumIndex (default rate)."""
    index = [
        make_premium_index(c["symbol"], (rates or {}).get(c["symbol"], "0.00010000"))
        for c in contracts
    ]
    return map_perpetuals(spot, {"symbols": list(contracts)}, index, now)


def mapped(spot: list[tuple[str, str]], *contracts: dict[str, Any]) -> dict[str, str]:
    rows, _ = run(spot, *contracts)
    return {r["spot_symbol"]: r["futures_symbol"] for r in rows}


def test_same_symbol_contract_is_mapped() -> None:
    rows, unmapped = run([("BTCUSDT", "BTC")], make_contract("BTC"))
    assert rows == [
        {
            "spot_symbol": "BTCUSDT",
            "futures_symbol": "BTCUSDT",
            "contract_multiplier": 1,
            "interest_rate_8h": "0.00010000",
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
    rows, _ = run([("PEPEUSDT", "PEPE")], make_contract(contract_base))
    assert (rows[0]["futures_symbol"], rows[0]["contract_multiplier"]) == (
        f"{contract_base}USDT",
        multiplier,
    )


def test_lookalike_symbols_are_not_matched() -> None:
    # Substrings of the symbol: a dominance index and another coin.
    spot = [("BTCUSDT", "BTC"), ("PUMPUSDT", "PUMP")]
    rows, unmapped = run(
        spot, make_contract("BTCDOM"), make_contract("PUMPBTC"), make_contract("BTC")
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
    rows, unmapped = run([("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")], contract, make_contract("ETH"))
    assert [r["spot_symbol"] for r in rows] == ["ETHUSDT"]
    assert unmapped == ["BTCUSDT"]


def test_two_contracts_for_one_coin_stop_the_mapping() -> None:
    with pytest.raises(ValueError, match=r"PEPEUSDT: several perpetuals \['1000PEPEUSDT', "):
        run([("PEPEUSDT", "PEPE")], make_contract("PEPE"), make_contract("1000PEPE"))


def test_rows_follow_whitelist_order() -> None:
    spot = [("SOLUSDT", "SOL"), ("BTCUSDT", "BTC")]
    assert list(mapped(spot, make_contract("BTC"), make_contract("SOL"))) == ["SOLUSDT", "BTCUSDT"]


def test_no_contract_for_any_symbol_raises() -> None:
    with pytest.raises(RuntimeError, match="No whitelisted symbol"):
        run([("BTCUSDT", "BTC")], make_contract("ETH"))


def test_naive_now_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        run([("BTCUSDT", "BTC")], make_contract("BTC"), now=datetime(2026, 1, 1))


def test_non_utc_now_is_normalized_to_utc() -> None:
    plus3 = NOW.astimezone(timezone(timedelta(hours=3)))
    rows, _ = run([("BTCUSDT", "BTC")], make_contract("BTC"), now=plus3)
    assert rows[0]["mapped_at_utc"] == "2026-01-01T00:00:00+00:00"


def test_interest_rate_is_each_contracts_own() -> None:
    # Live 2026-11-21: BNBUSDT has interestRate 0, the other 22 contracts 0.0001.
    rows, _ = run(
        [("BTCUSDT", "BTC"), ("BNBUSDT", "BNB")],
        make_contract("BTC"),
        make_contract("BNB"),
        rates={"BNBUSDT": "0.00000000"},
    )
    assert {r["futures_symbol"]: r["interest_rate_8h"] for r in rows} == {
        "BTCUSDT": "0.00010000",
        "BNBUSDT": "0.00000000",
    }


def test_interest_rate_is_looked_up_by_the_contract_not_the_spot_symbol() -> None:
    rows, _ = run(
        [("PEPEUSDT", "PEPE")], make_contract("1000PEPE"), rates={"1000PEPEUSDT": "0.00020000"}
    )
    assert rows[0]["interest_rate_8h"] == "0.00020000"


def test_mapped_contract_missing_from_premium_index_stops_the_mapping() -> None:
    exchange_info = {"symbols": [make_contract("BTC"), make_contract("ETH")]}
    only_eth = [make_premium_index("ETHUSDT")]
    with pytest.raises(ValueError, match="BTCUSDT: BTCUSDT is not in premiumIndex"):
        map_perpetuals([("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")], exchange_info, only_eth, NOW)


def test_unmapped_contracts_in_premium_index_are_ignored() -> None:
    # premiumIndex lists every contract of the exchange; only the mapped ones matter.
    exchange_info = {"symbols": [make_contract("BTC")]}
    index = [make_premium_index("BTCUSDT"), make_premium_index("DOGEUSDT", "0.00030000")]
    rows, _ = map_perpetuals([("BTCUSDT", "BTC")], exchange_info, index, NOW)
    assert [r["interest_rate_8h"] for r in rows] == ["0.00010000"]
