-- Initial schema.

-- One row per source file; makes reruns safe and gives a per-file summary.
CREATE TABLE ingest_file (
    id              BIGSERIAL PRIMARY KEY,
    path            TEXT        NOT NULL UNIQUE,
    checksum        TEXT,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    rows_total      BIGINT      NOT NULL DEFAULT 0,
    rows_inserted   BIGINT      NOT NULL DEFAULT 0,
    rows_rejected   BIGINT      NOT NULL DEFAULT 0,
    rows_duplicate  BIGINT      NOT NULL DEFAULT 0,
    rows_conflict   BIGINT      NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error           TEXT
);

-- Target table. The primary key enforces "one reading per sensor per instant".
-- sensor_name is stored directly (not normalised into a sensor table) so the
-- config-driven loader can map CSV columns straight onto table columns.
CREATE TABLE sensor_reading (
    sensor_name     TEXT             NOT NULL CHECK (sensor_name <> ''),
    ts              TIMESTAMPTZ      NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    ingest_file_id  BIGINT           NOT NULL REFERENCES ingest_file (id),
    line_no         BIGINT           NOT NULL,
    PRIMARY KEY (sensor_name, ts)
);

-- Readings arrive roughly in time order, so BRIN gives cheap time-range scans.
CREATE INDEX sensor_reading_ts_brin ON sensor_reading USING BRIN (ts);

-- Rows that failed validation, kept verbatim for auditing / replay.
CREATE TABLE rejected_record (
    id              BIGSERIAL PRIMARY KEY,
    ingest_file_id  BIGINT      NOT NULL REFERENCES ingest_file (id),
    line_no         BIGINT      NOT NULL,
    raw_line        TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX rejected_record_file_idx ON rejected_record (ingest_file_id);

-- Same unique key, different payload. First committed row wins; the loser is logged here
-- next to the row that was kept. Identical duplicates are only counted, not logged.
-- Key and values are JSONB so this table works for any dataset defined in the YAML config;
-- *_values hold the non-key columns.
CREATE TABLE ingest_conflict (
    id                  BIGSERIAL PRIMARY KEY,
    target_table        TEXT        NOT NULL,
    unique_key          JSONB       NOT NULL,
    kept_values         JSONB       NOT NULL,
    rejected_values     JSONB       NOT NULL,
    kept_file_id        BIGINT      NOT NULL REFERENCES ingest_file (id),
    kept_line_no        BIGINT      NOT NULL,
    rejected_file_id    BIGINT      NOT NULL REFERENCES ingest_file (id),
    rejected_line_no    BIGINT      NOT NULL,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ingest_conflict_file_idx ON ingest_conflict (rejected_file_id);
