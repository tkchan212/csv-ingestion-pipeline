-- Initial schema.

-- One row per source file; makes reruns safe and gives a per-file summary.
-- A file is identified by its name, not its full path, so the same directory mounted at
-- different paths (on the host, in Docker) is still recognised. File names are assumed
-- never to be reused: a new file with an old name would be skipped as already loaded.
-- While a worker loads a file it holds a session advisory lock on the file's id, so
-- no other worker (or concurrent run) loads it at the same time.
-- A file is loaded in batches. Each batch commits together with committed_line_no and
-- the counters, so they always describe exactly what is in the database, and a retry
-- resumes after committed_line_no.
CREATE TABLE ingest_file (
    id                BIGSERIAL PRIMARY KEY,
    file_name         TEXT        NOT NULL UNIQUE,   -- name within the input directory
    status            TEXT        NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    committed_line_no BIGINT      NOT NULL DEFAULT 0,
    rows_total        BIGINT      NOT NULL DEFAULT 0,
    rows_inserted     BIGINT      NOT NULL DEFAULT 0,
    rows_rejected     BIGINT      NOT NULL DEFAULT 0,
    rows_quarantined  BIGINT      NOT NULL DEFAULT 0,
    rows_duplicate    BIGINT      NOT NULL DEFAULT 0,
    rows_conflict     BIGINT      NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    error             TEXT
);

-- Reference data, maintained in config/reference/*.csv and loaded by `make seed`.
-- Ingestion only reads these tables; it never adds sensors.
CREATE TABLE sensor_type (
    code            TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    unit            TEXT NOT NULL,
    -- Physically plausible readings; anything outside is quarantined.
    valid_min       DOUBLE PRECISION,
    valid_max       DOUBLE PRECISION,
    CHECK (valid_min IS NULL OR valid_max IS NULL OR valid_min <= valid_max)
);

CREATE TABLE building (
    code            TEXT PRIMARY KEY,
    name            TEXT NOT NULL
);

-- A sensor's name (e.g. HQT-TMP-03-01) is the key the CSV files use.
CREATE TABLE sensor (
    name            TEXT PRIMARY KEY CHECK (name <> ''),
    type_code       TEXT NOT NULL REFERENCES sensor_type (code),
    building_code   TEXT NOT NULL REFERENCES building (code),
    floor           SMALLINT,          -- 0 = ground floor, negative = basement
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sensor_location_idx ON sensor (building_code, floor);
CREATE INDEX sensor_type_idx ON sensor (type_code);

-- Target table. The primary key enforces "one reading per sensor per instant" and
-- also serves per-sensor time-range queries.
CREATE TABLE sensor_reading (
    sensor_name     TEXT             NOT NULL REFERENCES sensor (name),
    ts              TIMESTAMPTZ      NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    ingest_file_id  BIGINT           NOT NULL REFERENCES ingest_file (id),
    line_no         BIGINT           NOT NULL,
    PRIMARY KEY (sensor_name, ts)
);

-- Readings arrive roughly in time order, so BRIN gives cheap time-range scans.
CREATE INDEX sensor_reading_ts_brin ON sensor_reading USING BRIN (ts);

-- Readings with what they mean: type, unit and location.
CREATE VIEW sensor_reading_detail AS
SELECT r.ts, r.sensor_name, r.value, t.unit, t.name AS sensor_type,
       b.name AS building, s.floor
FROM sensor_reading r
JOIN sensor s ON s.name = r.sensor_name
JOIN sensor_type t ON t.code = s.type_code
JOIN building b ON b.code = s.building_code;

-- Rows that could not be parsed (bad timestamp, non-numeric value, wrong field
-- count), kept verbatim for auditing.
CREATE TABLE rejected_record (
    id              BIGSERIAL PRIMARY KEY,
    ingest_file_id  BIGINT      NOT NULL REFERENCES ingest_file (id),
    line_no         BIGINT      NOT NULL,
    raw_line        TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A line is recorded at most once, even if a batch were ever replayed.
    UNIQUE (ingest_file_id, line_no)
);

-- Rows that parsed fine but failed a check against the reference data (unknown sensor,
-- implausible value). Kept parsed, so they can be reviewed and replayed once the cause
-- is fixed, e.g. the sensor is added to the registry. `record` holds the dataset's
-- columns as JSONB, like ingest_conflict, so this works for any configured dataset.
CREATE TABLE quarantined_reading (
    id              BIGSERIAL PRIMARY KEY,
    ingest_file_id  BIGINT      NOT NULL REFERENCES ingest_file (id),
    line_no         BIGINT      NOT NULL,
    check_name      TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    record          JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingest_file_id, line_no)
);
CREATE INDEX quarantined_reading_check_idx ON quarantined_reading (check_name);

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
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (rejected_file_id, rejected_line_no)
);
