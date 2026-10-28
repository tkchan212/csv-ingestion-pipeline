"""Files are committed batch by batch; failures keep finished batches and retries resume."""

import psycopg
import pytest

from ingestor.pipeline import process_file
from ingestor.writer import StagedLoad
from tests.conftest import make_config

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("test_sensors")]

HEADER = "timestamp,sensorName,value"

# Nine data rows (lines 2-10) covering every outcome, in batches of 2:
# line 6 conflicts with line 2 and line 8 duplicates line 3, both across batches.
MIXED = [
    HEADER,
    "2024-03-01T10:00:00Z,s1,1",      # 2  inserted
    "2024-03-01T10:01:00Z,s1,2",      # 3  inserted
    "not-a-date,s1,3",                # 4  rejected
    "2024-03-01T10:03:00Z,s9,4",      # 5  quarantined: unknown sensor
    "2024-03-01T10:00:00Z,s1,99",     # 6  conflict with line 2
    "2024-03-01T10:05:00Z,s2,500",    # 7  quarantined: out of range
    "2024-03-01T10:01:00Z,s1,2",      # 8  duplicate of line 3
    "2024-03-01T10:07:00Z,s2,7",      # 9  inserted
    "2024-03-01T10:08:00Z,s3,8",      # 10 inserted
]
EXPECTED = {"rows_total": 9, "rows_inserted": 4, "rows_rejected": 1, "rows_quarantined": 2,
            "rows_duplicate": 1, "rows_conflict": 1}


@pytest.fixture
def config(tmp_path, database_url):
    return make_config(tmp_path, database_url, batch_size=2)


def file_row(conn):
    return conn.execute(
        "SELECT status, committed_line_no, rows_total, rows_inserted, rows_rejected,"
        " rows_quarantined, rows_duplicate, rows_conflict FROM ingest_file"
    ).fetchone()


def table_counts(conn) -> dict[str, int]:
    tables = ("sensor_reading", "rejected_record", "quarantined_reading", "ingest_conflict")
    return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}


CLEAN_TABLES = {"sensor_reading": 4, "rejected_record": 1, "quarantined_reading": 2,
                "ingest_conflict": 1}


@pytest.fixture
def fail_on_batch(monkeypatch):
    """Make the n-th commit_batch call raise before it touches the database."""
    def install(n: int, action=None):
        real = StagedLoad.commit_batch
        calls = {"n": 0}

        def commit_batch(self, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == n:
                if action:
                    action(self)
                raise RuntimeError(f"injected failure in batch {n}")
            return real(self, *args, **kwargs)

        monkeypatch.setattr(StagedLoad, "commit_batch", commit_batch)
        return lambda: monkeypatch.setattr(StagedLoad, "commit_batch", real)
    return install


def test_clean_load_counts_every_outcome_across_batches(conn, config, write_csv) -> None:
    result = process_file(conn, write_csv("f.csv", MIXED), config)

    assert result.status == "done"
    assert {k: getattr(result, k) for k in EXPECTED} == EXPECTED
    assert file_row(conn)[:2] == ("done", 10)
    assert table_counts(conn) == CLEAN_TABLES
    # First line still wins when the later copy is in a later batch.
    assert conn.execute(
        "SELECT value, line_no FROM sensor_reading WHERE ts = '2024-03-01T10:00:00Z'"
    ).fetchone() == (1.0, 2)


def test_failure_keeps_committed_batches(conn, config, write_csv, fail_on_batch) -> None:
    fail_on_batch(3)

    result = process_file(conn, write_csv("f.csv", MIXED), config)

    assert result.status == "failed"
    assert "injected failure in batch 3" in result.error
    # Batches 1-2 (lines 2-5) are in, with counts that match exactly what is stored.
    assert file_row(conn) == ("failed", 5, 4, 2, 1, 1, 0, 0)
    assert table_counts(conn) == {"sensor_reading": 2, "rejected_record": 1,
                                  "quarantined_reading": 1, "ingest_conflict": 0}


def test_retry_resumes_and_ends_like_a_clean_load(conn, config, write_csv,
                                                   fail_on_batch) -> None:
    path = write_csv("f.csv", MIXED)
    restore = fail_on_batch(3)
    process_file(conn, path, config)
    restore()

    result = process_file(conn, path, config)

    assert result.status == "done"
    assert {k: getattr(result, k) for k in EXPECTED} == EXPECTED
    assert table_counts(conn) == CLEAN_TABLES    # nothing written twice


def test_connection_lost_mid_file_resumes_on_next_run(conn, config, write_csv, fail_on_batch,
                                                      database_url) -> None:
    path = write_csv("f.csv", MIXED)
    worker = psycopg.connect(database_url, autocommit=True)
    restore = fail_on_batch(4, action=lambda load: worker.close())

    with pytest.raises(psycopg.OperationalError):
        process_file(worker, path, config)   # cannot even record the failure
    restore()

    # Left as 'processing' with 3 batches (lines 2-7) committed; its lock died with it.
    assert file_row(conn)[:2] == ("processing", 7)
    result = process_file(conn, path, config)

    assert result.status == "done"
    assert {k: getattr(result, k) for k in EXPECTED} == EXPECTED
    assert table_counts(conn) == CLEAN_TABLES


def test_corruption_deep_in_a_large_file_keeps_the_good_part(conn, database_url, tmp_path,
                                                             write_csv) -> None:
    config = make_config(tmp_path, database_url, batch_size=1_000)
    path = write_csv("big.csv", [
        HEADER, *(f"2024-03-01T{i // 3600:02d}:{i // 60 % 60:02d}:{i % 60:02d}Z,s1,1"
                  for i in range(20_000)),
    ])
    with path.open("ab") as f:
        f.write(b"\xff\xfe not utf-8\n")

    result = process_file(conn, path, config)

    assert result.status == "failed"
    assert "UnicodeDecodeError" in result.error
    status, committed, *_ = file_row(conn)
    assert committed > 1 and result.rows_inserted == committed - 1   # one row per line
