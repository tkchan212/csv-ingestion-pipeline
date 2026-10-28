from datetime import UTC, datetime

import pytest

from ingestor.pipeline import process_file
from tests.conftest import make_config

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("test_sensors")]

HEADER = "timestamp,sensorName,value"


@pytest.fixture
def config(tmp_path, database_url):
    # Small batches so multi-batch staging is exercised by every test.
    return make_config(tmp_path, database_url, batch_size=2)


def readings(conn):
    return conn.execute(
        "SELECT sensor_name, ts, value, line_no FROM sensor_reading ORDER BY sensor_name, ts"
    ).fetchall()


def test_valid_rows_are_loaded_and_invalid_rows_recorded(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [
        HEADER,
        "2024-03-01T10:00:00Z,s1,1.5",
        "2024-03-01T10:00:00,s1,2",          # no time zone
        "2024-03-01T10:01:00Z,s1,ABC",
        "2024-03-01T10:02:00Z,s2,-4",
        "2024-03-01T10:03:00Z,s2",           # missing field
    ])

    result = process_file(conn, path, config)

    assert (result.status, result.rows_total, result.rows_inserted, result.rows_rejected) == (
        "done", 5, 2, 3
    )
    assert readings(conn) == [
        ("s1", datetime(2024, 3, 1, 10, 0, tzinfo=UTC), 1.5, 2),
        ("s2", datetime(2024, 3, 1, 10, 2, tzinfo=UTC), -4.0, 5),
    ]
    assert conn.execute(
        "SELECT line_no, raw_line, reason FROM rejected_record ORDER BY line_no"
    ).fetchall() == [
        (3, "2024-03-01T10:00:00,s1,2", "timestamp: timestamp has no time zone"),
        (4, "2024-03-01T10:01:00Z,s1,ABC", "value: not a number"),
        (6, "2024-03-01T10:03:00Z,s2", "expected 3 fields, got 2"),
    ]
    assert conn.execute(
        "SELECT status, rows_total, rows_inserted, rows_rejected, rows_duplicate, rows_conflict"
        " FROM ingest_file"
    ).fetchone() == ("done", 5, 2, 3, 0, 0)


def test_duplicates_within_a_file_keep_the_first_line(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [
        HEADER,
        "2024-03-01T10:00:00Z,s1,1",
        "2024-03-01T10:00:00Z,s1,1",         # exact duplicate
        "2024-03-01T18:00:00+08:00,s1,1.0",  # same instant and value, written differently
    ])

    result = process_file(conn, path, config)

    assert (result.rows_inserted, result.rows_duplicate, result.rows_conflict) == (1, 2, 0)
    assert readings(conn) == [("s1", datetime(2024, 3, 1, 10, 0, tzinfo=UTC), 1.0, 2)]


def test_duplicates_across_files_keep_the_row_loaded_first(conn, config, write_csv) -> None:
    first = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    second = write_csv("f2.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1", "2024-03-01T10:05:00Z,s1,3"])

    process_file(conn, first, config)
    result = process_file(conn, second, config)

    assert (result.rows_inserted, result.rows_duplicate, result.rows_conflict) == (1, 1, 0)
    assert [r[2] for r in readings(conn)] == [1.0, 3.0]


def test_rerunning_a_loaded_file_is_skipped(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])

    process_file(conn, path, config)
    result = process_file(conn, path, config)

    assert result.status == "skipped"
    assert len(readings(conn)) == 1


def test_bad_header_fails_the_file_and_loads_nothing(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", ["time,sensor,value", "2024-03-01T10:00:00Z,s1,1"])

    result = process_file(conn, path, config)

    assert result.status == "failed"
    assert "header is missing column(s): timestamp, sensorName" in result.error
    assert readings(conn) == []
    assert conn.execute("SELECT status, error FROM ingest_file").fetchone()[0] == "failed"


def test_failed_file_is_retried_on_the_next_run(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", ["wrong,header,here"])
    process_file(conn, path, config)

    write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    result = process_file(conn, path, config)

    assert result.status == "done"
    assert len(readings(conn)) == 1


def test_same_file_under_another_path_is_recognised(conn, config, write_csv, tmp_path) -> None:
    """The host and a container see the input directory at different paths."""
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1", "bad,s1,2"])
    elsewhere = tmp_path / "mounted-elsewhere"
    elsewhere.mkdir()
    copy = elsewhere / "f1.csv"
    copy.write_bytes(path.read_bytes())

    process_file(conn, path, config)
    result = process_file(conn, copy, config)

    assert result.status == "skipped"
    assert conn.execute("SELECT file_name FROM ingest_file").fetchall() == [("f1.csv",)]
    assert conn.execute("SELECT count(*) FROM rejected_record").fetchone() == (1,)
