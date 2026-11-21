"""Tests for the local environment check: variables, key file permissions, landing zones."""

from __future__ import annotations

import dataclasses
import stat
from pathlib import Path

import pytest

from ingestion import check_environment as ce
from tests.factories import START_MS, make_marked_batch

UID = 50000
REGULAR_FILE = stat.S_IFREG


def full_env(tmp_path: Path) -> dict[str, str]:
    """Every required variable set; the key paths point at real files."""
    env = {name: f"value-of-{name}" for name in ce.REQUIRED_VARS}
    for var in ce.KEY_PATH_VARS:
        key = tmp_path / f"{var.lower()}.p8"
        key.write_text("key")
        key.chmod(0o600)  # owner-only on POSIX; on Windows only the read-only bit is touched
        env[var] = str(key)
    return env


@pytest.fixture
def landing_zones(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Every dataset's landing zone moved under tmp_path, each with one complete batch."""
    roots = []
    datasets = []
    for dataset in ce.DATASETS:
        root = tmp_path / "raw" / dataset.name
        make_marked_batch(root, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
        roots.append(root)
        datasets.append(dataclasses.replace(dataset, landing_dir=root))
    monkeypatch.setattr(ce, "DATASETS", tuple(datasets))
    return roots


def test_no_missing_variables() -> None:
    assert ce.missing_variables({"A": "1", "B": "2"}, ["A", "B"]) == []


def test_empty_variable_counts_as_missing_and_order_is_kept() -> None:
    assert ce.missing_variables({"A": "", "C": "3"}, ["C", "B", "A"]) == ["B", "A"]


def test_required_variables_cover_loader_and_dbt() -> None:
    assert set(ce.REQUIRED_VARS) >= {
        "SNOWFLAKE_PRIVATE_KEY_PATH",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
        "SNOWFLAKE_DBT_PRIVATE_KEY_PATH",
        "DBT_ENV_SECRET_SNOWFLAKE_DBT_KEY_PASSPHRASE",
    }


@pytest.mark.parametrize("mode", [0o400, 0o600], ids=["read-only", "read-write"])
def test_owner_only_key_is_private(mode: int) -> None:
    assert ce.key_permission_problem(REGULAR_FILE | mode, UID, UID) is None


@pytest.mark.parametrize(
    "mode",
    [0o777, 0o640, 0o604, 0o620],
    ids=["windows-mount", "group-read", "others-read", "group-write"],
)
def test_key_open_to_group_or_others_is_reported(mode: int) -> None:
    problem = ce.key_permission_problem(REGULAR_FILE | mode, UID, UID)
    assert problem == f"mode {mode:04o}: group or others have access"


def test_key_owned_by_another_user_is_reported() -> None:
    problem = ce.key_permission_problem(REGULAR_FILE | 0o400, 0, UID)
    assert problem == "owned by uid 0, not by this process (uid 50000)"


def test_missing_key_file_is_reported(tmp_path: Path) -> None:
    assert ce.key_file_problem(tmp_path / "nope.p8") == "file not found"


def test_landing_zone_without_directory_is_reported(tmp_path: Path) -> None:
    assert ce.landing_zone_problem(tmp_path / "raw" / "klines_1h") == "directory not found"


def test_landing_zone_with_only_incomplete_batches_is_reported(tmp_path: Path) -> None:
    (tmp_path / "20250101T000000Z").mkdir()  # files but no manifest yet
    assert ce.landing_zone_problem(tmp_path) == "no complete batch"


def test_landing_zone_with_a_complete_batch_is_fine(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
    assert ce.landing_zone_problem(tmp_path) is None


def test_main_passes_on_a_ready_environment(
    tmp_path: Path, landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert ce.main(full_env(tmp_path)) == 0
    out = capsys.readouterr().out
    assert f"Variables: {len(ce.REQUIRED_VARS)} of {len(ce.REQUIRED_VARS)} set" in out
    assert "Landing zone klines: 1 symbols, newest 2025-01-01 00:00 UTC" in out
    assert "OK: environment is ready" in out


def test_main_names_missing_variables_and_never_prints_values(
    tmp_path: Path, landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    env = full_env(tmp_path)
    env["SNOWFLAKE_USER"] = ""
    del env["DBT_ENV_SECRET_SNOWFLAKE_DBT_KEY_PASSPHRASE"]
    assert ce.main(env) == 1
    out = capsys.readouterr().out
    assert "variable SNOWFLAKE_USER is not set" in out
    assert "variable DBT_ENV_SECRET_SNOWFLAKE_DBT_KEY_PASSPHRASE is not set" in out
    assert "value-of-" not in out


def test_main_fails_on_a_missing_key_file(
    tmp_path: Path, landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    env = full_env(tmp_path)
    env["SNOWFLAKE_DBT_PRIVATE_KEY_PATH"] = str(tmp_path / "nope.p8")
    assert ce.main(env) == 1
    assert "key SNOWFLAKE_DBT_PRIVATE_KEY_PATH: file not found" in capsys.readouterr().out


def test_main_fails_on_a_landing_zone_without_complete_batches(
    tmp_path: Path, landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    funding = landing_zones[1]
    for batch_dir in funding.iterdir():
        (batch_dir / "_manifest.json").unlink()
    assert ce.main(full_env(tmp_path)) == 1
    assert "landing zone funding_rate" in capsys.readouterr().out
