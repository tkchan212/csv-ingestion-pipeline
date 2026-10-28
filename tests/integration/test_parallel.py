"""Several worker processes, and several runs, loading at the same time."""

from dataclasses import replace

import psycopg
import pytest

from ingestor.config import ProcessingConfig
from ingestor.pipeline import process_file, run
from ingestor.writer import claim_file
from tests.conftest import make_config

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("test_sensors")]

HEADER = "timestamp,sensorName,value"


def ts(n: int) -> str:
    return f"2024-03-01T{n // 3600:02d}:{n // 60 % 60:02d}:{n % 60:02d}Z"


@pytest.fixture
def config(tmp_path, database_url):
    base = make_config(tmp_path, database_url)
    return replace(base, processing=ProcessingConfig(workers=3, batch_size=50))


def count(conn, sql: str) -> int:
    return conn.execute(sql).fetchone()[0]


def test_pool_loads_overlapping_files_exactly_once(conn, config, write_csv) -> None:
    # Six files: 100 keys of their own each, plus 50 shared keys that every file
    # writes with its own value. Each shared key must be kept once and lose 5 times.
    for f in range(6):
        own = [f"{ts(10_000 + f * 100 + i)},s1,{i % 50}" for i in range(100)]
        shared = [f"{ts(i)},s2,{f}" for i in range(50)]
        write_csv(f"f{f}.csv", [HEADER, *own, *shared])

    results = run(config)

    assert [r.status for r in results] == ["done"] * 6
    assert count(conn, "SELECT count(*) FROM sensor_reading") == 6 * 100 + 50
    assert sum(r.rows_inserted for r in results) == 650
    assert sum(r.rows_conflict for r in results) == 50 * 5
    assert count(conn, "SELECT count(*) FROM ingest_conflict") == 250
    # Whichever file committed first won each shared key, and every loser was logged
    # against the value that was kept.
    assert count(conn, """
        SELECT count(*) FROM ingest_conflict c
        JOIN sensor_reading r ON r.sensor_name = c.unique_key->>'sensor_name'
                             AND r.ts = (c.unique_key->>'ts')::timestamptz
        WHERE (c.kept_values->>'value')::float <> r.value
    """) == 0


def test_same_keys_in_opposite_orders_do_not_deadlock(conn, config, write_csv) -> None:
    keys = [f"{ts(i)},s{1 + i % 3},1" for i in range(300)]
    for f in range(4):
        write_csv(f"f{f}.csv", [HEADER, *(keys if f % 2 else reversed(keys))])

    results = run(replace(config, processing=ProcessingConfig(workers=4, batch_size=50)))

    assert [r.status for r in results] == ["done"] * 4
    assert sum(r.rows_inserted for r in results) == 300
    assert sum(r.rows_duplicate for r in results) == 900


def test_one_bad_file_does_not_stop_the_pool(conn, config, write_csv) -> None:
    write_csv("a.csv", [HEADER, f"{ts(1)},s1,1"])
    write_csv("b.csv", ["wrong,header"])
    write_csv("c.csv", [HEADER, f"{ts(2)},s1,2"])

    results = {r.path.name: r.status for r in run(config)}

    assert results == {"a.csv": "done", "b.csv": "failed", "c.csv": "done"}


def test_file_held_by_another_worker_is_skipped(conn, config, write_csv, database_url) -> None:
    path = write_csv("a.csv", [HEADER, f"{ts(1)},s1,1"])

    with psycopg.connect(database_url, autocommit=True) as other_worker:
        assert claim_file(other_worker, path.name).file_id is not None
        result = process_file(conn, path, config)

    assert result.status == "skipped"
    assert count(conn, "SELECT count(*) FROM sensor_reading") == 0


def test_file_left_by_a_crashed_worker_is_picked_up_again(conn, config, write_csv,
                                                          database_url) -> None:
    path = write_csv("a.csv", [HEADER, f"{ts(1)},s1,1"])
    crashed = psycopg.connect(database_url, autocommit=True)
    claim_file(crashed, path.name)
    crashed.close()        # the connection dies without releasing anything

    assert count(conn, "SELECT count(*) FROM ingest_file WHERE status = 'processing'") == 1
    results = run(config)

    assert [r.status for r in results] == ["done"]
    assert count(conn, "SELECT count(*) FROM sensor_reading") == 1


def test_two_concurrent_runs_load_each_file_once(conn, config, write_csv) -> None:
    for f in range(6):
        write_csv(f"f{f}.csv", [HEADER, *(f"{ts(f * 1000 + i)},s1,1" for i in range(200))])

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(2) as runs:
        first, second = runs.map(lambda _: run(config), range(2))

    statuses = sorted(r.status for r in first + second)
    assert statuses.count("done") == 6 and statuses.count("skipped") == 6
    assert count(conn, "SELECT count(*) FROM sensor_reading") == 1200
    assert count(conn, "SELECT sum(rows_inserted) FROM ingest_file") == 1200
