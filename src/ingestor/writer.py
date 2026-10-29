"""Load validated records into PostgreSQL.

A file is loaded in batches, one transaction each (StagedLoad.commit_batch):
COPY the batch into a temporary staging table, then merge staging into the
target table:

1. move rows that fail a reference-data check (checks.py) to quarantined_reading;
2. insert the first remaining row per unique key;
3. log remaining rows that collided with a kept row but had different values;
4. advance the file's checkpoint (ingest_file.committed_line_no) and counters.

Because step 4 commits with steps 1-3, a batch is applied exactly once: a crash
mid-batch rolls the whole batch back, and a retry resumes after the checkpoint.
Batches are taken in line order, so "the first line wins" still holds across
batches of one file.

The connection must be opened with autocommit=True; transactions are the
caller's explicit `conn.transaction()` blocks.

The target table is expected to have the dataset's columns plus two lineage
columns, `ingest_file_id` and `line_no`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from psycopg import Connection, sql

from ingestor.checks import CHECKS
from ingestor.config import PipelineConfig
from ingestor.validation import Rejection, ValidRecord

STAGING = sql.Identifier("ingest_staging")


@dataclass(frozen=True)
class TargetTable:
    name: str
    columns: tuple[str, ...]      # data columns, in COPY order
    unique_key: tuple[str, ...]
    checks: tuple[str, ...] = ()  # names in checks.CHECKS, run in this order

    @classmethod
    def from_config(cls, config: PipelineConfig) -> TargetTable:
        return cls(
            name=config.target.table,
            columns=tuple(c.target for c in config.dataset.columns),
            unique_key=config.dataset.unique_key,
            checks=config.dataset.checks,
        )

    @property
    def value_columns(self) -> tuple[str, ...]:
        return tuple(c for c in self.columns if c not in self.unique_key)


@dataclass(frozen=True)
class MergeCounts:
    staged: int               # rows left after quarantine
    inserted: int
    conflict: int             # same key, different values: logged to ingest_conflict
    quarantined: int = 0      # failed a reference-data check

    @property
    def duplicate(self) -> int:
        """Same key, same values as the kept row."""
        return self.staged - self.inserted - self.conflict


def _identifiers(names: Sequence[str]) -> sql.Composed:
    return sql.SQL(", ").join(map(sql.Identifier, names))


def _json_of(alias: str, names: Sequence[str]) -> sql.Composed:
    """jsonb_build_object('a', alias.a, 'b', alias.b, ...)"""
    pairs = (sql.SQL("{}, {}.{}").format(sql.Literal(n), sql.Identifier(alias), sql.Identifier(n))
             for n in names)
    return sql.SQL("jsonb_build_object({})").format(sql.SQL(", ").join(pairs))


@dataclass(frozen=True)
class BatchCounts:
    rows: int                 # data rows in the batch, rejected ones included
    rejected: int
    merge: MergeCounts


@dataclass(frozen=True)
class FileCounts:
    """What ingest_file says about a file, across all attempts."""

    status: str
    committed_line_no: int
    rows_total: int
    rows_inserted: int
    rows_rejected: int
    rows_quarantined: int
    rows_duplicate: int
    rows_conflict: int
    error: str | None


@dataclass(frozen=True)
class Claim:
    file_id: int | None           # None: do not load
    skip_reason: str | None = None
    resume_after: int = 0         # lines up to here are already committed


def claim_file(conn: Connection, file_name: str) -> Claim:
    """Take exclusive ownership of `file_name` for this connection's session.

    The owner holds a session-level advisory lock keyed on the file's id until
    release_file(). A worker that dies loses its connection and with it the lock,
    so its file (left as 'processing') is picked up again by the next run, while a
    worker that is merely slow keeps it. No timeouts or heartbeats are needed.
    """
    (file_id,) = conn.execute(
        """
        INSERT INTO ingest_file (file_name) VALUES (%s)
        ON CONFLICT (file_name) DO UPDATE SET file_name = EXCLUDED.file_name
        RETURNING id
        """,
        (file_name,),
    ).fetchone()
    (lock_acquired,) = conn.execute("SELECT pg_try_advisory_lock(%s)", (file_id,)).fetchone()
    if not lock_acquired:
        return Claim(None, "being loaded by another worker")
    # Checked only once the lock is held: another worker may have just finished it.
    (status, committed_line_no) = conn.execute(
        "SELECT status, committed_line_no FROM ingest_file WHERE id = %s", (file_id,)
    ).fetchone()
    if status == "done":
        release_file(conn, file_id)
        return Claim(None, "already ingested")
    conn.execute(
        """
        UPDATE ingest_file
        SET status = 'processing', started_at = now(), finished_at = NULL, error = NULL
        WHERE id = %s
        """,
        (file_id,),
    )
    return Claim(file_id, resume_after=committed_line_no)


def release_file(conn: Connection, file_id: int) -> None:
    conn.execute("SELECT pg_advisory_unlock(%s)", (file_id,))


def mark_done(conn: Connection, file_id: int) -> None:
    conn.execute(
        "UPDATE ingest_file SET status = 'done', finished_at = now() WHERE id = %s", (file_id,)
    )


def read_counts(conn: Connection, file_id: int) -> FileCounts:
    row = conn.execute(
        """
        SELECT status, committed_line_no, rows_total, rows_inserted, rows_rejected,
               rows_quarantined, rows_duplicate, rows_conflict, error
        FROM ingest_file WHERE id = %s
        """,
        (file_id,),
    ).fetchone()
    return FileCounts(*row)


def mark_failed(conn: Connection, file_id: int, error: str) -> None:
    conn.execute(
        "UPDATE ingest_file SET status = 'failed', finished_at = now(), error = %s WHERE id = %s",
        (error, file_id),
    )


class StagedLoad:
    """One file's load, batch by batch. Use as a context manager: the staging table
    lives for the session and is emptied at every commit."""

    def __init__(self, conn: Connection, table: TargetTable, file_id: int) -> None:
        self._conn = conn
        self._table = table
        self._file_id = file_id
        # Timestamps in the JSON of logged rows are rendered in the session time zone.
        conn.execute("SET TimeZone = 'UTC'")
        # Copies the target's column types without its constraints, so duplicates
        # can land in staging and be resolved by the merge.
        conn.execute(
            sql.SQL(
                "CREATE TEMP TABLE {staging} ON COMMIT DELETE ROWS AS "
                "SELECT {columns}, line_no FROM {table} WITH NO DATA"
            ).format(
                staging=STAGING,
                columns=_identifiers(table.columns),
                table=sql.Identifier(table.name),
            )
        )

    def __enter__(self) -> StagedLoad:
        return self

    def __exit__(self, *exc_info: object) -> None:
        if not self._conn.closed:  # a dead connection takes its temp table with it
            self._conn.execute(sql.SQL("DROP TABLE IF EXISTS {staging}").format(staging=STAGING))

    def commit_batch(
        self, records: Sequence[ValidRecord], rejections: Sequence[Rejection], through_line: int
    ) -> BatchCounts:
        """Load one batch and advance the checkpoint to `through_line`, atomically."""
        with self._conn.transaction():
            self.stage(records)
            self.reject(rejections)
            merge = self.merge()
            batch = BatchCounts(len(records) + len(rejections), len(rejections), merge)
            self._advance(through_line, batch)
        return batch

    def _advance(self, through_line: int, batch: BatchCounts) -> None:
        m = batch.merge
        self._conn.execute(
            """
            UPDATE ingest_file
            SET committed_line_no = %s,
                rows_total = rows_total + %s, rows_inserted = rows_inserted + %s,
                rows_rejected = rows_rejected + %s, rows_quarantined = rows_quarantined + %s,
                rows_duplicate = rows_duplicate + %s, rows_conflict = rows_conflict + %s
            WHERE id = %s
            """,
            (through_line, batch.rows, m.inserted, batch.rejected, m.quarantined,
             m.duplicate, m.conflict, self._file_id),
        )

    def stage(self, records: Sequence[ValidRecord]) -> None:
        if not records:
            return
        statement = sql.SQL("COPY {staging} ({columns}, line_no) FROM STDIN").format(
            staging=STAGING, columns=_identifiers(self._table.columns)
        )
        with self._conn.cursor().copy(statement) as copy:
            for record in records:
                copy.write_row([*(record.values[c] for c in self._table.columns), record.line_no])

    def reject(self, rejections: Sequence[Rejection]) -> None:
        if not rejections:
            return
        statement = "COPY rejected_record (ingest_file_id, line_no, raw_line, reason) FROM STDIN"
        with self._conn.cursor().copy(statement) as copy:
            for r in rejections:
                # PostgreSQL text cannot hold NUL bytes.
                copy.write_row([self._file_id, r.line_no, r.raw_line.replace("\x00", "\\x00"),
                                r.reason])

    def merge(self) -> MergeCounts:
        """Quarantine, then insert the first staged row per unique key, then log staged
        rows that lost to a kept row with different values.

        Quarantine runs first so an implausible reading can never become the "first"
        row for its key and push out a good one.

        Conflicts are logged after the insert for parallel workers. If another
        transaction holds an uncommitted row with the same key, the INSERT waits for
        it to finish. The conflict query that follows is a new statement with a new
        snapshot (READ COMMITTED), so it sees that row once committed. Checking for
        conflicts before inserting would miss it.
        """
        # Temp tables are never auto-analyzed; without stats the planner may pick
        # nested loops over a million-row staging table.
        self._conn.execute(sql.SQL("ANALYZE {staging}").format(staging=STAGING))
        params = {"file_id": self._file_id}
        quarantined = sum(
            self._conn.execute(self._quarantine_sql(name), params).rowcount
            for name in self._table.checks
        )
        inserted = self._conn.execute(self._insert_sql(), params).rowcount
        conflict = 0
        if self._table.value_columns:     # a key-only dataset cannot have conflicts
            conflict = self._conn.execute(self._conflict_sql(), params).rowcount
        (staged,) = self._conn.execute(
            sql.SQL("SELECT count(*) FROM {staging}").format(staging=STAGING)
        ).fetchone()
        return MergeCounts(
            staged=staged, inserted=inserted, conflict=conflict, quarantined=quarantined
        )

    def _quarantine_sql(self, name: str) -> sql.Composed:
        # Moves failing rows out of staging; a row failing several checks is recorded
        # once, under the first check that caught it.
        check = CHECKS[name]
        return sql.SQL(
            """
            WITH moved AS (
                DELETE FROM {staging} AS s WHERE {fails_when} RETURNING s.*
            )
            INSERT INTO quarantined_reading (ingest_file_id, line_no, check_name, reason, record)
            SELECT %(file_id)s, s.line_no, {name}, {reason}, {record}
            FROM moved AS s
            """
        ).format(
            staging=STAGING,
            fails_when=sql.SQL(check.fails_when),
            name=sql.Literal(name),
            reason=sql.Literal(check.reason),
            record=_json_of("s", self._table.columns),
        )

    def _insert_sql(self) -> sql.Composed:
        return sql.SQL(
            """
            INSERT INTO {table} ({columns}, ingest_file_id, line_no)
            SELECT DISTINCT ON ({key}) {columns}, %(file_id)s, line_no
            FROM {staging}
            ORDER BY {key}, line_no
            ON CONFLICT ({key}) DO NOTHING
            """
        ).format(
            table=sql.Identifier(self._table.name),
            columns=_identifiers(self._table.columns),
            key=_identifiers(self._table.unique_key),
            staging=STAGING,
        )

    def _conflict_sql(self) -> sql.Composed:
        key, values = self._table.unique_key, self._table.value_columns

        def row_of(alias: str) -> sql.Composed:
            return sql.SQL("ROW({})").format(sql.SQL(", ").join(
                sql.SQL("{}.{}").format(sql.Identifier(alias), sql.Identifier(n)) for n in values
            ))

        # Every staged row joins its kept row in the target (from this file or an
        # earlier one). The kept row itself, and rows equal to it, are not conflicts.
        return sql.SQL(
            """
            INSERT INTO ingest_conflict (
                target_table, unique_key, kept_values, rejected_values,
                kept_file_id, kept_line_no, rejected_file_id, rejected_line_no
            )
            SELECT {table_name}, {key_json}, {kept_json}, {staged_json},
                   t.ingest_file_id, t.line_no, %(file_id)s, s.line_no
            FROM {staging} AS s
            JOIN {table} AS t USING ({key})
            WHERE (t.ingest_file_id, t.line_no) <> (%(file_id)s, s.line_no)
              AND {kept_row} IS DISTINCT FROM {staged_row}
            """
        ).format(
            table_name=sql.Literal(self._table.name),
            key_json=_json_of("s", key),
            kept_json=_json_of("t", values),
            staged_json=_json_of("s", values),
            staging=STAGING,
            table=sql.Identifier(self._table.name),
            key=_identifiers(key),
            kept_row=row_of("t"),
            staged_row=row_of("s"),
        )
