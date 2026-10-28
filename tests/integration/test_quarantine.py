"""Rows that parse but make no sense are quarantined, never loaded or used to change reference
data."""

from dataclasses import replace

import psycopg
import pytest

from ingestor.pipeline import process_file
from tests.conftest import make_config

pytestmark = pytest.mark.integration

HEADER = "timestamp,sensorName,value"


@pytest.fixture
def config(tmp_path, database_url):
    return make_config(tmp_path, database_url, batch_size=2)


@pytest.fixture(autouse=True)
def registry(conn) -> None:
    conn.execute(
        "INSERT INTO sensor_type VALUES ('TMP', 'Air temperature', '°C', -20, 60),"
        " ('NOR', 'No range', 'u', NULL, NULL)"
    )
    conn.execute("INSERT INTO building (code, name) VALUES ('HQT', 'Harbour Quay Tower')")
    conn.execute(
        "INSERT INTO sensor (name, type_code, building_code, floor)"
        " VALUES ('HQT-TMP-03-01', 'TMP', 'HQT', 3), ('HQT-NOR-03-01', 'NOR', 'HQT', 3)"
    )


def quarantined(conn):
    return conn.execute(
        "SELECT line_no, check_name, reason, record FROM quarantined_reading ORDER BY line_no"
    ).fetchall()


def loaded(conn):
    return conn.execute(
        "SELECT sensor_name, value, line_no FROM sensor_reading ORDER BY line_no"
    ).fetchall()


def test_unknown_sensor_is_quarantined_and_not_registered(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [
        HEADER,
        "2024-03-01T10:00:00Z,HQT-TMP-03-01,21.5",
        "2024-03-01T18:00:00+08:00,HQT-TMP-99-01,22",
    ])

    result = process_file(conn, path, config)

    assert (result.rows_inserted, result.rows_quarantined) == (1, 1)
    assert quarantined(conn) == [(
        3, "known_sensor", "sensor is not in the registry",
        {"ts": "2024-03-01T10:00:00+00:00", "sensor_name": "HQT-TMP-99-01", "value": 22},
    )]
    assert conn.execute("SELECT count(*) FROM sensor").fetchone() == (2,)
    assert conn.execute(
        "SELECT rows_quarantined FROM ingest_file"
    ).fetchone() == (1,)


@pytest.mark.parametrize("value, ok", [
    ("-20", True), ("60", True), ("21.5", True),     # bounds are inclusive
    ("-20.1", False), ("60.5", False), ("999", False), ("-273", False),
])
def test_value_outside_the_type_range_is_quarantined(conn, config, write_csv, value, ok) -> None:
    path = write_csv("f1.csv", [HEADER, f"2024-03-01T10:00:00Z,HQT-TMP-03-01,{value}"])

    result = process_file(conn, path, config)

    assert (result.rows_inserted, result.rows_quarantined) == ((1, 0) if ok else (0, 1))
    if not ok:
        assert quarantined(conn)[0][1:3] == (
            "value_in_type_range", "value is outside the valid range for the sensor type"
        )


def test_type_without_a_range_accepts_any_value(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,HQT-NOR-03-01,123456"])

    assert process_file(conn, path, config).rows_inserted == 1


def test_implausible_row_does_not_win_its_key(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [
        HEADER,
        "2024-03-01T10:00:00Z,HQT-TMP-03-01,999",    # glitch first
        "2024-03-01T10:00:00Z,HQT-TMP-03-01,21.5",   # then the real reading
    ])

    result = process_file(conn, path, config)

    assert (result.rows_inserted, result.rows_quarantined, result.rows_conflict) == (1, 1, 0)
    assert loaded(conn) == [("HQT-TMP-03-01", 21.5, 3)]


def test_parse_errors_are_rejected_not_quarantined(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,HQT-TMP-99-01,ABC"])

    result = process_file(conn, path, config)

    assert (result.rows_rejected, result.rows_quarantined) == (1, 0)


def test_quarantine_rolls_back_with_a_failed_file(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,HQT-TMP-99-01,1"])
    path.write_bytes(path.read_bytes() + b"\xff not utf-8\n")

    assert process_file(conn, path, config).status == "failed"
    assert quarantined(conn) == []


def test_without_the_check_an_unknown_sensor_fails_the_file(conn, config, write_csv) -> None:
    """The foreign key is the safety net: nothing unknown ever reaches sensor_reading."""
    config = replace(config, dataset=replace(config.dataset, checks=()))
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,HQT-TMP-99-01,1"])

    result = process_file(conn, path, config)

    assert result.status == "failed"
    assert psycopg.errors.ForeignKeyViolation.__name__ in result.error
    assert loaded(conn) == []
