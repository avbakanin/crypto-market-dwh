"""Interval lengths are checked against independent literals, not against the code's own math."""

from ingestion import extract_klines


def test_interval_lengths_match_binance_definitions() -> None:
    assert extract_klines.INTERVAL_MS_BY_NAME == {"1h": 3_600_000, "1d": 86_400_000}
