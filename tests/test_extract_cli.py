"""Tests for the extracts' command line: the exit code tells a batch from 'nothing new'."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from ingestion import (
    extract_funding,
    extract_klines,
    extract_long_short,
    extract_open_interest,
)
from ingestion.extract_cli import parse_extract_args

SKIP = 99  # what the Airflow DAG asks for; BashOperator reports it as skipped

# Each extract: its module, the name of its symbol list loader and of its client class.
EXTRACTS = [
    pytest.param(extract_klines, "load_symbols", "BinanceClient", id="klines"),
    pytest.param(extract_funding, "load_contracts", "BinanceFuturesClient", id="funding"),
    pytest.param(extract_open_interest, "load_contracts", "BinanceFuturesClient", id="oi"),
    pytest.param(extract_long_short, "load_contracts", "BinanceFuturesClient", id="long_short"),
]


class OfflineClient:
    """Stands in for a Binance client: main only opens and closes it."""

    def __enter__(self) -> OfflineClient:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def offline_main(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    loader: str,
    client: str,
    batch_dir: Path | None,
) -> Callable[[list[str]], int]:
    """main of an extract whose extract_batch returns batch_dir; no network, no landing zone."""
    monkeypatch.setattr(module, loader, lambda: ["BTCUSDT"])
    monkeypatch.setattr(module, client, OfflineClient)

    def fake_extract_batch(*args: Any, **kwargs: Any) -> Path | None:
        return batch_dir

    monkeypatch.setattr(module, "extract_batch", fake_extract_batch)
    return module.main


@pytest.mark.parametrize(("module", "loader", "client"), EXTRACTS)
def test_nothing_new_is_a_success_by_default(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, loader: str, client: str
) -> None:
    main = offline_main(monkeypatch, module, loader, client, batch_dir=None)
    assert main([]) == 0


@pytest.mark.parametrize(("module", "loader", "client"), EXTRACTS)
def test_nothing_new_ends_with_the_code_asked_for(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, loader: str, client: str
) -> None:
    main = offline_main(monkeypatch, module, loader, client, batch_dir=None)
    assert main(["--nothing-new-exit-code", str(SKIP)]) == SKIP


@pytest.mark.parametrize(("module", "loader", "client"), EXTRACTS)
def test_a_written_batch_is_a_success_whatever_the_code_asked_for(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, loader: str, client: str, tmp_path: Path
) -> None:
    main = offline_main(monkeypatch, module, loader, client, batch_dir=tmp_path)
    assert main(["--nothing-new-exit-code", str(SKIP)]) == 0


@pytest.mark.parametrize(("module", "loader", "client"), EXTRACTS)
def test_bad_exit_code_is_rejected_before_any_work(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, loader: str, client: str
) -> None:
    def loader_must_not_run() -> list[str]:
        raise AssertionError("symbols loaded before the arguments were checked")

    main = offline_main(monkeypatch, module, loader, client, batch_dir=None)
    monkeypatch.setattr(module, loader, loader_must_not_run)
    with pytest.raises(SystemExit) as raised:
        main(["--nothing-new-exit-code", "256"])
    assert raised.value.code == 2  # argparse's usage error


@pytest.mark.parametrize("code", [0, 255])
def test_exit_code_range_edges_are_accepted(code: int) -> None:
    args = parse_extract_args("x", ["--nothing-new-exit-code", str(code)])
    assert args.nothing_new_exit_code == code


@pytest.mark.parametrize("code", [-1, 256])
def test_exit_code_outside_a_byte_is_rejected(code: int) -> None:
    with pytest.raises(SystemExit):
        parse_extract_args("x", ["--nothing-new-exit-code", str(code)])
