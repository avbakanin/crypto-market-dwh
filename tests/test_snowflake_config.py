"""Tests for building Snowflake connection settings from the environment (no network)."""

from pathlib import Path

import pytest

from ingestion.clients import snowflake_conn
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


def test_connect_passes_params_and_sets_keep_alive_detection_explicitly(
    env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake_connect(**kwargs: object) -> str:
        captured.update(kwargs)
        return "connection"

    def no_dotenv(*args: object, **kwargs: object) -> bool:
        raise AssertionError("an explicit env must not read .env")

    monkeypatch.setattr(snowflake_conn.snowflake.connector, "connect", fake_connect)
    monkeypatch.setattr(snowflake_conn, "load_dotenv", no_dotenv)

    assert snowflake_conn.connect(env) == "connection"
    assert captured == {
        **connection_params(env),
        "enable_server_session_keep_alive_auto_detection": False,
    }


# --- reporter: fixed identity, account and key from the environment ---------------------


@pytest.fixture
def reporter_env(env: dict[str, str], key_file: Path) -> dict[str, str]:
    """The loader's variables are present too, as in .env and in the scheduler."""
    return {
        **env,
        snowflake_conn.REPORTER_KEY_PATH_VAR: str(key_file),
        snowflake_conn.REPORTER_PASSPHRASE_VAR: SECRET,
    }


def test_reporter_identity_is_fixed_whatever_the_loader_variables_say(
    reporter_env: dict[str, str], key_file: Path
) -> None:
    assert snowflake_conn.reporter_connection_params(reporter_env) == {
        "account": "ORG-ACCOUNT",
        "user": "CRYPTO_REPORTER",
        "role": "CRYPTO_REPORTER",
        "warehouse": "CRYPTO_WH",
        "database": "CRYPTO_DWH",
        "schema": "MARTS",
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(key_file),
        "private_key_password": SECRET,
        "arrow_number_to_decimal": True,
    }


def test_reporter_never_uses_the_loader_key(reporter_env: dict[str, str], tmp_path: Path) -> None:
    reporter_env["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(tmp_path / "loader_key.p8")
    reporter_env[PASSPHRASE_VAR] = "loader-passphrase"
    params = snowflake_conn.reporter_connection_params(reporter_env)
    assert params["private_key_file"] == reporter_env[snowflake_conn.REPORTER_KEY_PATH_VAR]
    assert params["private_key_password"] == SECRET


@pytest.mark.parametrize("name", snowflake_conn.REPORTER_REQUIRED_VARS)
def test_reporter_missing_var_is_named(reporter_env: dict[str, str], name: str) -> None:
    reporter_env[name] = ""
    with pytest.raises(SnowflakeConfigError, match=name):
        snowflake_conn.reporter_connection_params(reporter_env)


def test_reporter_missing_key_file_fails_before_connecting(
    reporter_env: dict[str, str], tmp_path: Path
) -> None:
    reporter_env[snowflake_conn.REPORTER_KEY_PATH_VAR] = str(tmp_path / "no_such_key.p8")
    with pytest.raises(SnowflakeConfigError, match="Private key file not found"):
        snowflake_conn.reporter_connection_params(reporter_env)


def test_connect_builds_the_reporter_params_when_asked(
    reporter_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake_connect(**kwargs: object) -> str:
        captured.update(kwargs)
        return "connection"

    monkeypatch.setattr(snowflake_conn.snowflake.connector, "connect", fake_connect)
    snowflake_conn.connect(reporter_env, params=snowflake_conn.reporter_connection_params)
    assert captured["user"] == "CRYPTO_REPORTER"
    assert captured["enable_server_session_keep_alive_auto_detection"] is False
