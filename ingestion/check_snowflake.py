"""Smoke check of the loader's Snowflake connection: who the session is, and what it can be.

Run from repo root: python -m ingestion.check_snowflake
Exit code 1 if the session can use roles beyond the project role (least privilege).
Only context functions are queried, so the warehouse is not resumed.
"""

from __future__ import annotations

import json
import sys

from ingestion.clients.snowflake_conn import connect

EXPECTED_ROLES = frozenset({"CRYPTO_DWH_ROLE", "PUBLIC"})  # PUBLIC is granted to every user

CONTEXT_FUNCTIONS = (
    "CURRENT_ACCOUNT_NAME()",
    "CURRENT_REGION()",
    "CURRENT_USER()",
    "CURRENT_ROLE()",
    "CURRENT_SECONDARY_ROLES()",
    "CURRENT_AVAILABLE_ROLES()",
    "CURRENT_WAREHOUSE()",
    "CURRENT_DATABASE()",
    "CURRENT_SCHEMA()",
    "CURRENT_VERSION()",
)


def unexpected_roles(available_roles_json: str) -> set[str]:
    """Roles the session could switch to beyond EXPECTED_ROLES."""
    return set(json.loads(available_roles_json)) - EXPECTED_ROLES


def main() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(CONTEXT_FUNCTIONS)}")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Context query returned no rows")

    context = dict(zip(CONTEXT_FUNCTIONS, row, strict=True))
    for name, value in context.items():
        print(f"{name:<28} {value}")

    extra = unexpected_roles(context["CURRENT_AVAILABLE_ROLES()"])
    if extra:
        print(f"\nFAIL: session can use roles beyond {sorted(EXPECTED_ROLES)}: {sorted(extra)}")
        return 1
    print("\nOK: connected with the project role only")
    return 0


if __name__ == "__main__":
    sys.exit(main())
