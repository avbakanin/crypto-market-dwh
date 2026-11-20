"""Snowflake connection for the loader: settings from the environment, key-pair auth.

Secrets never live in code: values come from environment variables, which
connect() loads from the repo's .env file (see .env.example).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
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


class SnowflakeConfigError(RuntimeError):
    """Connection settings are missing or invalid. Messages name variables, never values."""


def connection_params(env: Mapping[str, str]) -> dict[str, str]:
    """Build snowflake.connector.connect() arguments from environment variables."""
    missing = [name for name in REQUIRED_VARS if not env.get(name)]
    if missing:
        raise SnowflakeConfigError(
            f"Missing environment variables: {', '.join(missing)} (see .env.example)"
        )

    key_path = Path(env["SNOWFLAKE_PRIVATE_KEY_PATH"]).expanduser()
    if not key_path.is_file():
        raise SnowflakeConfigError(f"Private key file not found: {key_path}")

    params = {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "user": env["SNOWFLAKE_USER"],
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(key_path),
        "role": env["SNOWFLAKE_ROLE"],
        "warehouse": env["SNOWFLAKE_WAREHOUSE"],
        "database": env["SNOWFLAKE_DATABASE"],
        "schema": env["SNOWFLAKE_SCHEMA"],
    }
    passphrase = env.get(PASSPHRASE_VAR)
    if passphrase:
        params["private_key_password"] = passphrase
    return params


def connect(env: Mapping[str, str] | None = None) -> SnowflakeConnection:
    """Open a connection; by default reads .env, without overriding variables already set."""
    if env is None:
        load_dotenv(ENV_PATH)
        env = os.environ
    return snowflake.connector.connect(
        **connection_params(env),
        # Connector 5.x warns unless set explicitly. False = always log out on close:
        # the future default, and the loader has no async queries to preserve.
        enable_server_session_keep_alive_auto_detection=False,
    )
