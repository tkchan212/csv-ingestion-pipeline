"""Same sensor and instant, different value: first committed row wins, the other is logged."""

import threading
import time

import psycopg
import pytest

from ingestor.pipeline import process_file
from ingestor.writer import StagedLoad, TargetTable, claim_file
from tests.conftest import make_config

pytestmark = pytest.mark.integration

HEADER = "timestamp,sensorName,value"
KEY = {"sensor_name": "s1", "ts": "2024-03-01T10:00:00+00:00"}


@pytest.fixture
def config(tmp_path, database_url):
    return make_config(tmp_path, database_url, batch_size=2)


def conflicts(conn):
    return conn.execute(
        """
        SELECT target_table, unique_key, kept_values, rejected_values,
               kf.path, c.kept_line_no, rf.path, c.rejected_line_no
        FROM ingest_conflict c
        JOIN ingest_file kf ON kf.id = c.kept_file_id
        JOIN ingest_file rf ON rf.id = c.rejected_file_id
        ORDER BY c.id
        """
    ).fetchall()


def stored_value(conn) -> float:
    return conn.execute("SELECT value FROM sensor_reading").fetchone()[0]


def test_conflict_within_a_file_logs_both_values(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [
        HEADER,
        "2024-03-01T10:00:00Z,s1,1",
        "2024-03-01T10:00:00Z,s1,1",         # duplicate: counted, not logged
        "2024-03-01T18:00:00+08:00,s1,9",    # same instant, different value: conflict
    ])

    result = process_file(conn, path, config)

    assert (result.rows_inserted, result.rows_duplicate, result.rows_conflict) == (1, 1, 1)
    assert stored_value(conn) == 1.0
    file = str(path.resolve())
    assert conflicts(conn) == [
        ("sensor_reading", KEY, {"value": 1}, {"value": 9}, file, 2, file, 4),
    ]
    assert conn.execute("SELECT rows_conflict FROM ingest_file").fetchone() == (1,)


def test_conflict_across_files_points_at_the_kept_row(conn, config, write_csv) -> None:
    first = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    second = write_csv("f2.csv", [HEADER, "2024-03-01T10:05:00Z,s1,5", "2024-03-01T10:00:00Z,s1,2"])

    process_file(conn, first, config)
    result = process_file(conn, second, config)

    assert (result.rows_inserted, result.rows_duplicate, result.rows_conflict) == (1, 0, 1)
    assert conflicts(conn) == [
        ("sensor_reading", KEY, {"value": 1}, {"value": 2},
         str(first.resolve()), 2, str(second.resolve()), 3),
    ]


def test_every_losing_value_is_logged(conn, config, write_csv) -> None:
    path = write_csv("f1.csv", [
        HEADER,
        "2024-03-01T10:00:00Z,s1,1",
        "2024-03-01T10:00:00Z,s1,2",
        "2024-03-01T10:00:00Z,s1,3",
    ])

    result = process_file(conn, path, config)

    assert result.rows_conflict == 2
    assert [(c[2], c[3]) for c in conflicts(conn)] == [
        ({"value": 1}, {"value": 2}),
        ({"value": 1}, {"value": 3}),
    ]


def test_failed_file_leaves_no_conflicts_behind(conn, config, write_csv) -> None:
    write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    process_file(conn, config.source.directory / "f1.csv", config)
    second = write_csv("f2.csv", [HEADER, "2024-03-01T10:00:00Z,s1,2"])
    second.write_bytes(second.read_bytes() + b"\xff\xfe not utf-8\n")

    result = process_file(conn, second, config)

    assert result.status == "failed"
    assert conflicts(conn) == []


def test_conflict_with_a_concurrent_uncommitted_file_is_still_logged(
    conn, config, write_csv, database_url
) -> None:
    """Worker A has inserted the key but not committed when worker B merges the same key.

    B's INSERT blocks on A's row; once A commits, B must log the conflict.
    """
    file_a = write_csv("a.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    file_b = write_csv("b.csv", [HEADER, "2024-03-01T10:00:00Z,s1,2"])
    results = {}

    with psycopg.connect(database_url, autocommit=True) as worker_a:
        file_id = claim_file(worker_a, str(file_a.resolve()))
        with worker_a.transaction():
            load = StagedLoad(worker_a, TargetTable.from_config(config), file_id)
            load.stage([_record(2, "s1", "2024-03-01T10:00:00Z", 1.0)])
            load.merge()

            def run_b() -> None:
                with psycopg.connect(database_url, autocommit=True) as worker_b:
                    results["b"] = process_file(worker_b, file_b, config)

            thread = threading.Thread(target=run_b)
            thread.start()
            _wait_until_blocked_on_lock(conn)
        # A commits here.
        thread.join(timeout=10)

    assert results["b"].rows_conflict == 1
    assert stored_value(conn) == 1.0
    assert [(c[2], c[3]) for c in conflicts(conn)] == [({"value": 1}, {"value": 2})]


def _record(line_no, sensor, ts, value):
    from ingestor.validation import ValidRecord, parse_timestamp_tz

    return ValidRecord(line_no, {"ts": parse_timestamp_tz(ts), "sensor_name": sensor,
                                 "value": value})


def _wait_until_blocked_on_lock(conn, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if conn.execute(
            "SELECT 1 FROM pg_stat_activity"
            " WHERE wait_event_type = 'Lock' AND datname = current_database()"
        ).fetchone():
            return
        time.sleep(0.02)
    raise AssertionError("worker B never blocked on worker A's row")
