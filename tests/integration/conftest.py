"""Integration fixtures: a throwaway `<db>_test` database built from db/migrations."""

import os
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

MIGRATIONS = Path(__file__).parents[2] / "db" / "migrations"
DEFAULT_URL = "postgresql://ingestor:ingestor@localhost:5432/sensors"


@pytest.fixture(scope="session")
def database_url() -> str:
    admin_url = os.environ.get("DATABASE_URL", DEFAULT_URL)
    test_db = f"{conninfo_to_dict(admin_url).get('dbname', 'postgres')}_test"
    try:
        admin = psycopg.connect(admin_url, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable, run `make up` first ({exc})")
    with admin:
        name = sql.Identifier(test_db)
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(name))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(name))

    url = make_conninfo(admin_url, dbname=test_db)
    with psycopg.connect(url, autocommit=True) as conn:
        for migration in sorted(MIGRATIONS.glob("*.sql")):
            conn.execute(migration.read_text())
    return url


@pytest.fixture
def conn(database_url: str):
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(
            "TRUNCATE ingest_conflict, rejected_record, quarantined_reading, sensor_reading,"
            " ingest_file, sensor, building, sensor_type RESTART IDENTITY"
        )
        yield conn


@pytest.fixture
def test_sensors(conn) -> None:
    """Register sensors s1..s3 (type range -100..100) for tests about loading mechanics."""
    conn.execute("INSERT INTO sensor_type VALUES ('TST', 'Test', 'u', -100, 100)")
    conn.execute("INSERT INTO building (code, name) VALUES ('TB', 'Test building')")
    conn.execute(
        "INSERT INTO sensor (name, type_code, building_code, floor)"
        " VALUES ('s1', 'TST', 'TB', 1), ('s2', 'TST', 'TB', 1), ('s3', 'TST', 'TB', 1)"
    )
