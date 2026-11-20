"""Tests for the least-privilege rule of the Snowflake smoke check (no network)."""

from ingestion.check_snowflake import unexpected_roles


def test_project_role_and_public_are_expected() -> None:
    assert unexpected_roles('["CRYPTO_DWH_ROLE","PUBLIC"]') == set()


def test_admin_role_is_reported() -> None:
    assert unexpected_roles('["ACCOUNTADMIN","CRYPTO_DWH_ROLE","PUBLIC"]') == {"ACCOUNTADMIN"}
