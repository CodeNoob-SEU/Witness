"""Standalone migration entrypoint.

Applying schema changes from the application's startup path would couple a
rolling deploy to a schema upgrade. Migrations therefore run as their own job:
once, to completion, before the new application version starts.

    REACT_AGENT_POSTGRES_DSN=postgresql://... uv run react-agent-migrate

The DSN is read from the environment and never echoed, because a psycopg error
chain can otherwise carry the password into deployment logs.
"""

from __future__ import annotations

import asyncio
import os
import sys

from .postgres_journal import PostgresRunJournal


def _dsn_from_env() -> str | None:
    raw = os.getenv("REACT_AGENT_POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if raw is None:
        return None
    dsn = raw.strip()
    return dsn or None


async def _run() -> int:
    dsn = _dsn_from_env()
    if dsn is None:
        print(
            "Set REACT_AGENT_POSTGRES_DSN (or DATABASE_URL) to the target database.",
            file=sys.stderr,
        )
        return 2
    try:
        async with PostgresRunJournal(dsn) as journal:
            await journal.migrate()
    except Exception as exc:
        # Replace the whole chain: psycopg exceptions can include connection
        # parameters, and this runs in CI and deploy logs.
        print(f"Migration failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print("Migrations applied.")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
