"""Snowflake connections of the service users: settings from the environment, key-pair auth.

Secrets never live in code: values come from environment variables, which
connect() loads from the repo's .env file (see .env.example).

- The loader (connection_params): every setting from SNOWFLAKE_* variables.
- The reporter (reporter_connection_params): read-only export of the marts. Its user,
  role and namespace are fixed here, like dbt's in dbt/profiles.yml, so the export can
  only ever run as CRYPTO_REPORTER; the environment gives the account and the key only.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"

REQUIRED_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
)
PASSPHRASE_VAR = "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"  # optional: only for encrypted keys

REPORTER_KEY_PATH_VAR = "SNOWFLAKE_REPORTER_PRIVATE_KEY_PATH"
REPORTER_PASSPHRASE_VAR = "SNOWFLAKE_REPORTER_PRIVATE_KEY_PASSPHRASE"  # optional, as above
REPORTER_REQUIRED_VARS = ("SNOWFLAKE_ACCOUNT", REPORTER_KEY_PATH_VAR)
# snowflake/setup.sql, section 8: SELECT on MARTS and DQ, nothing else.
REPORTER_IDENTITY = {
    "user": "CRYPTO_REPORTER",
    "role": "CRYPTO_REPORTER",
    "warehouse": "CRYPTO_WH",
    "database": "CRYPTO_DWH",
    "schema": "MARTS",
}

ConnectionParams = dict[str, str | bool]


class SnowflakeConfigError(RuntimeError):
    """Connection settings are missing or invalid. Messages name variables, never values."""


def _require(env: Mapping[str, str], names: tuple[str, ...]) -> None:
    missing = [name for name in names if not env.get(name)]
    if missing:
        raise SnowflakeConfigError(
            f"Missing environment variables: {', '.join(missing)} (see .env.example)"
        )


def _key_pair(env: Mapping[str, str], path_var: str, passphrase_var: str) -> ConnectionParams:
    """Key-pair authentication settings; the key file must exist before connecting."""
    key_path = Path(env[path_var]).expanduser()
    if not key_path.is_file():
        raise SnowflakeConfigError(f"Private key file not found: {key_path}")
    params: ConnectionParams = {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(key_path),
    }
    passphrase = env.get(passphrase_var)
    if passphrase:
        params["private_key_password"] = passphrase
    return params


def connection_params(env: Mapping[str, str]) -> ConnectionParams:
    """The loader's snowflake.connector.connect() arguments from environment variables."""
    _require(env, REQUIRED_VARS)
    return {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "user": env["SNOWFLAKE_USER"],
        **_key_pair(env, "SNOWFLAKE_PRIVATE_KEY_PATH", PASSPHRASE_VAR),
        "role": env["SNOWFLAKE_ROLE"],
        "warehouse": env["SNOWFLAKE_WAREHOUSE"],
        "database": env["SNOWFLAKE_DATABASE"],
        "schema": env["SNOWFLAKE_SCHEMA"],
    }


def reporter_connection_params(env: Mapping[str, str]) -> ConnectionParams:
    """The reporter's connect() arguments: fixed identity, account and key from the env."""
    _require(env, REPORTER_REQUIRED_VARS)
    return {
        "account": env["SNOWFLAKE_ACCOUNT"],
        **REPORTER_IDENTITY,
        **_key_pair(env, REPORTER_KEY_PATH_VAR, REPORTER_PASSPHRASE_VAR),
        # Arrow results: fixed-point NUMBER as decimal128(precision, scale) in every
        # result. Without it the connector sends double or decimal128 depending on the
        # values of the result, so a file's schema could change from one export to the next.
        "arrow_number_to_decimal": True,
    }


def connect(
    env: Mapping[str, str] | None = None,
    params: Callable[[Mapping[str, str]], ConnectionParams] = connection_params,
) -> SnowflakeConnection:
    """Open a connection (the loader's by default); by default reads .env, without
    overriding variables already set."""
    if env is None:
        load_dotenv(ENV_PATH)
        env = os.environ
    return snowflake.connector.connect(
        **params(env),
        # Connector 5.x warns unless set explicitly. False = always log out on close:
        # the future default, and no service user here runs async queries.
        enable_server_session_keep_alive_auto_detection=False,
    )
