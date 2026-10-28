"""Load validated records into PostgreSQL.

Per file, in one transaction: COPY batches into a temporary staging table,
then merge staging into the target table, keeping the first row per unique key
and logging rows that collide with different values to ingest_conflict.

The connection must be opened with autocommit=True; transactions are the
caller's explicit `conn.transaction()` blocks.

The target table is expected to have the dataset's columns plus two lineage
columns, `ingest_file_id` and `line_no`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from psycopg import Connection, sql

from ingestor.config import PipelineConfig
from ingestor.validation import Rejection, ValidRecord

STAGING = sql.Identifier("ingest_staging")


@dataclass(frozen=True)
class TargetTable:
    name: str
    columns: tuple[str, ...]      # data columns, in COPY order
    unique_key: tuple[str, ...]

    @classmethod
    def from_config(cls, config: PipelineConfig) -> TargetTable:
        return cls(
            name=config.target.table,
            columns=tuple(c.target for c in config.dataset.columns),
            unique_key=config.dataset.unique_key,
        )

    @property
    def value_columns(self) -> tuple[str, ...]:
        return tuple(c for c in self.columns if c not in self.unique_key)


@dataclass(frozen=True)
class MergeCounts:
    staged: int
    inserted: int
    conflict: int             # same key, different values: logged to ingest_conflict

    @property
    def duplicate(self) -> int:
        """Same key, same values as the kept row."""
        return self.staged - self.inserted - self.conflict


def _identifiers(names: Sequence[str]) -> sql.Composed:
    return sql.SQL(", ").join(map(sql.Identifier, names))


def claim_file(conn: Connection, path: str) -> int | None:
    """Mark `path` as processing and return its id, or None if it was already loaded."""
    row = conn.execute(
        """
        INSERT INTO ingest_file (path, status, started_at)
        VALUES (%s, 'processing', now())
        ON CONFLICT (path) DO UPDATE
            SET status = 'processing', started_at = now(), finished_at = NULL, error = NULL
            WHERE ingest_file.status <> 'done'
        RETURNING id
        """,
        (path,),
    ).fetchone()
    return row[0] if row else None


def mark_done(
    conn: Connection,
    file_id: int,
    *,
    rows_total: int,
    rows_inserted: int,
    rows_rejected: int,
    rows_duplicate: int,
    rows_conflict: int,
) -> None:
    conn.execute(
        """
        UPDATE ingest_file
        SET status = 'done', finished_at = now(), rows_total = %s, rows_inserted = %s,
            rows_rejected = %s, rows_duplicate = %s, rows_conflict = %s
        WHERE id = %s
        """,
        (rows_total, rows_inserted, rows_rejected, rows_duplicate, rows_conflict, file_id),
    )


def mark_failed(conn: Connection, file_id: int, error: str) -> None:
    conn.execute(
        "UPDATE ingest_file SET status = 'failed', finished_at = now(), error = %s WHERE id = %s",
        (error, file_id),
    )


class StagedLoad:
    """One file's load. Create it inside a transaction; the staging table drops on commit."""

    def __init__(self, conn: Connection, table: TargetTable, file_id: int) -> None:
        self._conn = conn
        self._table = table
        self._file_id = file_id
        # Timestamps in the conflict log's JSON are rendered in the session time zone.
        conn.execute("SET LOCAL TimeZone = 'UTC'")
        # Copies the target's column types without its constraints, so duplicates
        # can land in staging and be resolved by the merge.
        conn.execute(
            sql.SQL(
                "CREATE TEMP TABLE {staging} ON COMMIT DROP AS "
                "SELECT {columns}, line_no FROM {table} WITH NO DATA"
            ).format(
                staging=STAGING,
                columns=_identifiers(table.columns),
                table=sql.Identifier(table.name),
            )
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
        """Insert the first staged row per unique key, then log staged rows that lost to a
        kept row with different values.

        The order matters for parallel workers. If another transaction holds an
        uncommitted row with the same key, the INSERT waits for it to finish. The
        conflict query that follows is a new statement with a new snapshot
        (READ COMMITTED), so it sees that row once committed. Checking for conflicts
        before inserting would miss it.
        """
        # Temp tables are never auto-analyzed; without stats the planner may pick
        # nested loops over a million-row staging table.
        self._conn.execute(sql.SQL("ANALYZE {staging}").format(staging=STAGING))
        params = {"file_id": self._file_id}
        inserted = self._conn.execute(self._insert_sql(), params).rowcount
        conflict = 0
        if self._table.value_columns:     # a key-only dataset cannot have conflicts
            conflict = self._conn.execute(self._conflict_sql(), params).rowcount
        (staged,) = self._conn.execute(
            sql.SQL("SELECT count(*) FROM {staging}").format(staging=STAGING)
        ).fetchone()
        return MergeCounts(staged=staged, inserted=inserted, conflict=conflict)

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

        def json_of(alias: str, names: Sequence[str]) -> sql.Composed:
            pairs = (sql.SQL("{}, {}.{}").format(sql.Literal(n), sql.Identifier(alias),
                                                 sql.Identifier(n)) for n in names)
            return sql.SQL("jsonb_build_object({})").format(sql.SQL(", ").join(pairs))

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
            key_json=json_of("s", key),
            kept_json=json_of("t", values),
            staged_json=json_of("s", values),
            staging=STAGING,
            table=sql.Identifier(self._table.name),
            key=_identifiers(key),
            kept_row=row_of("t"),
            staged_row=row_of("s"),
        )
