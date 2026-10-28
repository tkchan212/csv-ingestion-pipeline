"""Orchestration: discover files, fan them out to worker processes, collect results.

Each worker is a separate process with its own database connection and loads
whole files, one at a time. Processes rather than threads, because parsing and
validating rows is CPU-bound Python, which the GIL would otherwise serialise.
With a single worker everything runs in this process, which is simpler to debug.

Workers coordinate only through the database: per-file advisory locks
(writer.claim_file) and the target table's unique key. Workers inserting the same
keys lock them in key order (the merge sorts), so they cannot deadlock.
"""

from __future__ import annotations

import logging
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import Connection

from ingestor.config import PipelineConfig
from ingestor.reader import open_csv
from ingestor.scanner import discover_files
from ingestor.validation import RecordValidator, Rejection, ValidRecord
from ingestor.writer import (
    StagedLoad,
    TargetTable,
    claim_file,
    mark_done,
    mark_failed,
    read_counts,
    release_file,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileResult:
    path: Path
    status: str               # done | failed | skipped
    rows_total: int = 0
    rows_inserted: int = 0
    rows_rejected: int = 0
    rows_quarantined: int = 0
    rows_duplicate: int = 0
    rows_conflict: int = 0
    error: str | None = None


def run(config: PipelineConfig) -> list[FileResult]:
    if not config.source.directory.is_dir():
        # glob() on a missing directory silently finds nothing.
        raise NotADirectoryError(f"input directory not found: {config.source.directory}")
    files = discover_files(config.source.directory, config.source.pattern)
    workers = min(config.processing.workers, len(files)) or 1
    log.info("found %d file(s) in %s, %d worker(s)", len(files), config.source.directory, workers)

    started = time.monotonic()
    if workers == 1:
        with psycopg.connect(config.target.database_url, autocommit=True) as conn:
            results = [process_file(conn, path, config) for path in files]
    else:
        # Fail fast with a clear error rather than inside every worker.
        psycopg.connect(config.target.database_url, connect_timeout=10).close()
        results = _run_pool(files, config, workers)

    _log_summary(results, time.monotonic() - started)
    return results


def _run_pool(files: list[Path], config: PipelineConfig, workers: int) -> list[FileResult]:
    # "spawn" on every platform: workers start clean instead of inheriting this
    # process's state (Linux would otherwise fork).
    context = multiprocessing.get_context("spawn")
    results = []
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=context,
        initializer=_init_worker,
        initargs=(config, logging.getLogger().level),
    ) as pool:
        futures = {pool.submit(_process_in_worker, path): path for path in files}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                # The worker process died (e.g. killed). Its file stays 'processing'
                # and, the lock having died with the connection, is retried next run.
                error = f"worker crashed: {type(exc).__name__}: {exc}"
                log.error("%s: %s", futures[future].name, error)
                results.append(FileResult(futures[future], "failed", error=error))
    return results


# Per-worker-process state, set once by _init_worker.
_worker_conn: Connection | None = None
_worker_config: PipelineConfig | None = None


def _init_worker(config: PipelineConfig, log_level: int) -> None:
    global _worker_conn, _worker_config
    # Spawned processes start without the parent's logging setup.
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(processName)s %(name)s: %(message)s",
    )
    _worker_config = config
    _worker_conn = psycopg.connect(config.target.database_url, autocommit=True)


def _process_in_worker(path: Path) -> FileResult:
    return process_file(_worker_conn, path, _worker_config)


def _log_summary(results: list[FileResult], seconds: float) -> None:
    by_status = {s: sum(r.status == s for r in results) for s in ("done", "skipped", "failed")}
    inserted = sum(r.rows_inserted for r in results)
    log.info(
        "finished in %.1fs: %s; %d rows inserted",
        seconds, ", ".join(f"{n} {s}" for s, n in by_status.items()), inserted,
    )


def process_file(conn: Connection, path: Path, config: PipelineConfig) -> FileResult:
    """Load one file in batches, resuming after its last committed batch.

    A failure keeps the batches committed so far; the file is marked 'failed' and a
    later run continues after them. The counts returned cover all attempts.
    """
    path = path.resolve()
    # Identity is the name within the (flat) input directory, not the full path,
    # which differs between the host and a container.
    claim = claim_file(conn, path.name)
    if claim.file_id is None:
        log.info("%s: %s, skipping", path.name, claim.skip_reason)
        return FileResult(path, "skipped")
    file_id = claim.file_id
    if claim.resume_after:
        log.info("%s: resuming after line %d", path.name, claim.resume_after)

    try:
        _load(conn, path, file_id, claim.resume_after, config)
        mark_done(conn, file_id)
    except Exception as exc:  # one bad file must not stop the run
        mark_failed(conn, file_id, f"{type(exc).__name__}: {exc}")
    finally:
        release_file(conn, file_id)

    counts = read_counts(conn, file_id)
    result = FileResult(
        path,
        counts.status,
        rows_total=counts.rows_total,
        rows_inserted=counts.rows_inserted,
        rows_rejected=counts.rows_rejected,
        rows_quarantined=counts.rows_quarantined,
        rows_duplicate=counts.rows_duplicate,
        rows_conflict=counts.rows_conflict,
        error=counts.error,
    )
    if result.status == "failed":
        log.error("%s: failed after line %d: %s", path.name, counts.committed_line_no,
                  result.error)
    else:
        log.info(
            "%s: %d rows, %d inserted, %d rejected, %d quarantined, %d duplicate, %d conflict",
            path.name, result.rows_total, result.rows_inserted, result.rows_rejected,
            result.rows_quarantined, result.rows_duplicate, result.rows_conflict,
        )
    return result


def _load(
    conn: Connection, path: Path, file_id: int, resume_after: int, config: PipelineConfig
) -> None:
    batch_size = config.processing.batch_size
    records: list[ValidRecord] = []
    rejections: list[Rejection] = []
    last_line = resume_after

    with (
        StagedLoad(conn, TargetTable.from_config(config), file_id) as load,
        open_csv(path, config.source.encoding, config.source.delimiter) as source,
    ):
        validator = RecordValidator(config.dataset.columns, source.header)
        for row in source.rows():
            if row.line_no <= resume_after:
                continue  # committed by an earlier attempt
            outcome = validator.validate(row)
            (rejections if isinstance(outcome, Rejection) else records).append(outcome)
            last_line = row.line_no
            if len(records) + len(rejections) >= batch_size:
                load.commit_batch(records, rejections, last_line)
                records.clear()
                rejections.clear()
        if records or rejections:
            load.commit_batch(records, rejections, last_line)
