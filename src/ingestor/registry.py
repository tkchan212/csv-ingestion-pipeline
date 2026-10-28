"""Load reference data (sensor types, buildings, sensors) from CSV files.

Each file's header names the table's columns. Rows are upserted on the table's key,
so the registry is the source of truth: re-running it updates changed metadata.
It is the only way sensors are added; ingestion quarantines readings from sensors
it does not know. Reference files are small, so they are read whole.

    python -m ingestor.registry [config/reference] [--db-url URL]
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
from pathlib import Path

import psycopg
from psycopg import Connection, sql

log = logging.getLogger(__name__)

# In foreign-key order: (table, file, key columns).
TABLES = (
    ("sensor_type", "sensor_types.csv", ("code",)),
    ("building", "buildings.csv", ("code",)),
    ("sensor", "sensors.csv", ("name",)),
)


def load_registry(conn: Connection, directory: Path) -> dict[str, int]:
    """Upsert every reference file in one transaction; return rows per table."""
    with conn.transaction():
        return {
            table: upsert_csv(conn, table, directory / filename, key)
            for table, filename, key in TABLES
        }


def upsert_csv(conn: Connection, table: str, path: Path, key: tuple[str, ...]) -> int:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = [name.strip() for name in next(reader)]
        # Empty cells mean "unknown", e.g. a sensor with no floor.
        rows = [[cell.strip() or None for cell in row] for row in reader if row]

    missing = [k for k in key if k not in header]
    if missing:
        raise ValueError(f"{path.name}: header is missing key column(s): {', '.join(missing)}")
    others = [c for c in header if c not in key]
    statement = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({values}) "
        "ON CONFLICT ({key}) DO {action}"
    ).format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(map(sql.Identifier, header)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(header)),
        key=sql.SQL(", ").join(map(sql.Identifier, key)),
        action=sql.SQL("UPDATE SET {}").format(sql.SQL(", ").join(
            sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in others
        )) if others else sql.SQL("NOTHING"),
    )
    with conn.cursor() as cur:
        cur.executemany(statement, rows)
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load sensor reference data.")
    parser.add_argument("directory", type=Path, nargs="?", default=Path("config/reference"))
    parser.add_argument("--db-url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args(argv)
    if not args.db_url:
        parser.error("--db-url or DATABASE_URL is required")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    with psycopg.connect(args.db_url, autocommit=True) as conn:
        counts = load_registry(conn, args.directory)
    log.info("loaded %s", ", ".join(f"{n} {table}" for table, n in counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
