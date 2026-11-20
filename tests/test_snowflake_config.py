"""Tests for building Snowflake connection settings from the environment (no network)."""

from pathlib import Path

import pytest

from ingestion.clients.snowflake_conn import (
    PASSPHRASE_VAR,
    REQUIRED_VARS,
    SnowflakeConfigError,
    connection_params,
)

SECRET = "correct-horse-battery-staple"


@pytest.fixture
def key_file(tmp_path: Path) -> Path:
    path = tmp_path / "rsa_key.p8"
    path.write_text("not a real key", encoding="ascii")
    return path


@pytest.fixture
def env(key_file: Path) -> dict[str, str]:
    return {
        "SNOWFLAKE_ACCOUNT": "ORG-ACCOUNT",
        "SNOWFLAKE_USER": "CRYPTO_LOADER",
        "SNOWFLAKE_PRIVATE_KEY_PATH": str(key_file),
        "SNOWFLAKE_ROLE": "CRYPTO_DWH_ROLE",
        "SNOWFLAKE_WAREHOUSE": "CRYPTO_WH",
        "SNOWFLAKE_DATABASE": "CRYPTO_DWH",
        "SNOWFLAKE_SCHEMA": "RAW",
        PASSPHRASE_VAR: SECRET,
    }


def test_complete_env_maps_to_key_pair_params(env: dict[str, str], key_file: Path) -> None:
    assert connection_params(env) == {
        "account": "ORG-ACCOUNT",
        "user": "CRYPTO_LOADER",
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(key_file),
        "role": "CRYPTO_DWH_ROLE",
        "warehouse": "CRYPTO_WH",
        "database": "CRYPTO_DWH",
        "schema": "RAW",
        "private_key_password": SECRET,
    }


@pytest.mark.parametrize("value", [None, ""], ids=["absent", "empty"])
def test_passphrase_is_optional(env: dict[str, str], value: str | None) -> None:
    if value is None:
        del env[PASSPHRASE_VAR]
    else:
        env[PASSPHRASE_VAR] = value
    assert "private_key_password" not in connection_params(env)


@pytest.mark.parametrize("name", REQUIRED_VARS)
def test_missing_required_var_is_named(env: dict[str, str], name: str) -> None:
    del env[name]
    with pytest.raises(SnowflakeConfigError, match=name):
        connection_params(env)


def test_empty_value_counts_as_missing(env: dict[str, str]) -> None:
    env["SNOWFLAKE_ROLE"] = ""
    with pytest.raises(SnowflakeConfigError, match="SNOWFLAKE_ROLE"):
        connection_params(env)


def test_all_missing_vars_are_reported_at_once(env: dict[str, str]) -> None:
    del env["SNOWFLAKE_USER"]
    del env["SNOWFLAKE_SCHEMA"]
    with pytest.raises(SnowflakeConfigError) as exc:
        connection_params(env)
    assert "SNOWFLAKE_USER" in str(exc.value)
    assert "SNOWFLAKE_SCHEMA" in str(exc.value)


def test_error_message_never_contains_the_passphrase(env: dict[str, str]) -> None:
    del env["SNOWFLAKE_ACCOUNT"]
    with pytest.raises(SnowflakeConfigError) as exc:
        connection_params(env)
    assert SECRET not in str(exc.value)


def test_missing_key_file_fails_before_connecting(env: dict[str, str], tmp_path: Path) -> None:
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(tmp_path / "no_such_key.p8")
    with pytest.raises(SnowflakeConfigError, match="Private key file not found"):
        connection_params(env)


def test_key_path_expands_home_directory(
    env: dict[str, str], key_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(key_file.parent))
    monkeypatch.setenv("USERPROFILE", str(key_file.parent))  # expanduser() on Windows
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = "~/" + key_file.name
    assert connection_params(env)["private_key_file"] == str(key_file)
