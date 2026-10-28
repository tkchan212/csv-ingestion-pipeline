"""Orchestration: discover files, process each one, collect per-file results.

Files are processed one after another for now; the worker pool comes later and
reuses process_file unchanged (one connection per worker).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import Connection

from ingestor.config import PipelineConfig
from ingestor.reader import open_csv
from ingestor.scanner import discover_files
from ingestor.validation import RecordValidator, Rejection, ValidRecord
from ingestor.writer import StagedLoad, TargetTable, claim_file, mark_done, mark_failed

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileResult:
    path: Path
    status: str               # done | failed | skipped
    rows_total: int = 0
    rows_inserted: int = 0
    rows_rejected: int = 0
    rows_duplicate: int = 0
    rows_conflict: int = 0
    error: str | None = None


def run(config: PipelineConfig) -> list[FileResult]:
    files = discover_files(config.source.directory, config.source.pattern)
    log.info("found %d file(s) in %s", len(files), config.source.directory)
    with psycopg.connect(config.target.database_url, autocommit=True) as conn:
        results = [process_file(conn, path, config) for path in files]
    by_status = {s: sum(r.status == s for r in results) for s in ("done", "skipped", "failed")}
    log.info("finished: %s", ", ".join(f"{n} {s}" for s, n in by_status.items()))
    return results


def process_file(conn: Connection, path: Path, config: PipelineConfig) -> FileResult:
    """Load one file atomically: either all its rows and a 'done' status, or nothing."""
    path = path.resolve()
    file_id = claim_file(conn, str(path))
    if file_id is None:
        log.info("%s: already ingested, skipping", path.name)
        return FileResult(path, "skipped")

    try:
        with conn.transaction():
            result = _load(conn, path, file_id, config)
            mark_done(
                conn,
                file_id,
                rows_total=result.rows_total,
                rows_inserted=result.rows_inserted,
                rows_rejected=result.rows_rejected,
                rows_duplicate=result.rows_duplicate,
                rows_conflict=result.rows_conflict,
            )
    except Exception as exc:  # one bad file must not stop the run
        error = f"{type(exc).__name__}: {exc}"
        log.error("%s: failed: %s", path.name, error)
        mark_failed(conn, file_id, error)
        return FileResult(path, "failed", error=error)

    log.info(
        "%s: %d rows, %d inserted, %d rejected, %d duplicate, %d conflict",
        path.name, result.rows_total, result.rows_inserted,
        result.rows_rejected, result.rows_duplicate, result.rows_conflict,
    )
    return result


def _load(conn: Connection, path: Path, file_id: int, config: PipelineConfig) -> FileResult:
    load = StagedLoad(conn, TargetTable.from_config(config), file_id)
    batch_size = config.processing.batch_size
    records: list[ValidRecord] = []
    rejections: list[Rejection] = []
    total = rejected = 0

    def flush() -> None:
        load.stage(records)
        load.reject(rejections)
        records.clear()
        rejections.clear()

    with open_csv(path, config.source.encoding, config.source.delimiter) as source:
        validator = RecordValidator(config.dataset.columns, source.header)
        for row in source.rows():
            total += 1
            outcome = validator.validate(row)
            if isinstance(outcome, Rejection):
                rejections.append(outcome)
                rejected += 1
            else:
                records.append(outcome)
            if len(records) + len(rejections) >= batch_size:
                flush()
        flush()

    counts = load.merge()
    return FileResult(
        path,
        "done",
        rows_total=total,
        rows_inserted=counts.inserted,
        rows_rejected=rejected,
        rows_duplicate=counts.duplicate,
        rows_conflict=counts.conflict,
    )
