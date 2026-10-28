# CSV Ingestion Pipeline

Loads every CSV file in a directory into PostgreSQL, in parallel, with row validation,
duplicate handling and conflict logging. What a record looks like (columns, types,
validation rules, unique key) is defined in [config/pipeline.yaml](config/pipeline.yaml).

> Status: all four steps done; see "Limitations and next steps" for what is left.

## Quick start

```bash
make setup          # .env, local venv, PostgreSQL in Docker, reference data loaded
make generate-data  # mock CSVs into ./data/input
make ingest         # run the ingestor container against ./data/input
make psql           # inspect results
```

Run with other settings by overriding the config:

```bash
docker compose run --rm ingestor --config /app/config/pipeline.yaml --workers 8
```

Or run it on the host, against the database in Docker:

```bash
.venv/bin/csv-ingestor --input-dir data/input --db-url postgresql://ingestor:ingestor@localhost:5432/sensors
```

Exit codes: `0` every file loaded or skipped, `1` at least one file failed (see
`ingest_file.error`), `2` the run could not start (invalid config, missing directory,
database unreachable).

## Configuration

[config/pipeline.yaml](config/pipeline.yaml) has four sections: `source`, `target`,
`processing` and `dataset`. Strings may use `${VAR}` or `${VAR:-default}`, so secrets stay in
the environment. CLI flags (`--input-dir`, `--db-url`, `--workers`) override the file and
are applied before expansion, so `--db-url` works when `DATABASE_URL` is unset.

The file is validated before anything runs: unknown or misspelt settings, unknown types and
rules, a `unique_key` that is not a column, and so on. Every problem is reported in one go:

```
invalid configuration in config/pipeline.yaml:
  - target.database_url: environment variable DATABASE_URL is not set
  - processing.workers: must be a positive integer
```

To load a different dataset, point the `dataset` columns at its CSV headers and target
columns. A new value type means registering one parser function in `validation.py`.
The target table itself is a migration; the config never creates tables.

## Layout

```
config/pipeline.yaml      dataset + runtime configuration
config/reference/         sensor types, buildings, sensors (`make seed`)
db/migrations/            schema, applied by the Postgres container on first start
src/ingestor/
  cli.py                  flags -> config -> pipeline
  config.py               YAML loading, ${ENV} expansion, validation
  scanner.py              find input files
  reader.py               stream CSV rows (constant memory per file)
  validation.py           parsers + rules registries, row -> record | rejection
  checks.py               reference-data checks; failing rows are quarantined
  writer.py               COPY into staging, quarantine, merge into target, log conflicts
  registry.py             upsert reference data from config/reference/*.csv
  pipeline.py             worker pool, per-file status and summary
scripts/                  setup.sh, generate_data.py
tests/unit, tests/integration
```

## Schema

```
sensor_type (code PK, name, unit, valid_min, valid_max)    building (code PK, name)
        ^                                                        ^
sensor (name PK, type_code FK, building_code FK, floor)
        ^
sensor_reading (sensor_name FK, ts, value, ingest_file_id FK, line_no)   PK (sensor_name, ts)

ingest_file          one row per source file: status and counts
rejected_record      rows that could not be parsed, verbatim, with the reason
quarantined_reading  rows that parsed but failed a reference-data check, with the check
ingest_conflict      same key, different values: kept vs rejected, with their file and line
```

Every data row ends up in exactly one place:

| Outcome | Where | Example |
|---|---|---|
| loaded | `sensor_reading` | first valid reading for a sensor and instant |
| rejected | `rejected_record` | `yesterday,HQT-TMP-03-01,21.5`, `…,ABC` |
| quarantined | `quarantined_reading` | unknown sensor; 150 °C from a temperature sensor |
| duplicate | counted in `ingest_file.rows_duplicate` | same key, same value |
| conflict | `ingest_conflict` | same key, different value |

A sensor name such as `HQT-TMP-03-01` says little to someone who doesn't know the codes,
so each sensor has a type (with its unit), a building and a floor. The view
`sensor_reading_detail` shows readings with that context. The CSV files carry only the name.
The metadata comes from a registry, `config/reference/*.csv`, which `make seed` upserts.

Only `make seed` adds or changes sensors; ingestion never does. Bad data must not shape the
reference data.

### Quarantine

A row can parse fine and still make no sense. Before the insert, the merge runs the checks
listed under `dataset.checks` in the config, in the database against the reference tables:

- `known_sensor`: the sensor is in the registry.
- `value_in_type_range`: the value is within `sensor_type.valid_min..valid_max` (inclusive).
  For example, temperature is −20..60 °C and humidity 0..100 %. A type with no range is
  not checked.

A failing row is moved to `quarantined_reading` with the check's name, a reason and the
parsed record as JSONB, instead of being loaded. Each row is recorded once, under the first
check it fails. Quarantine runs before deduplication, so a glitch reading can never become
the "first" row for its key and push out the real one.

The foreign key from `sensor_reading` to `sensor` is the safety net. Even with the checks
turned off, an unknown sensor fails the file instead of slipping in.

Checks are named SQL conditions in `checks.py`, switched on by name in the config, like
parsers and rules. They depend on the dataset's columns and reference tables, so a new
dataset brings its own.

```sql
SELECT check_name, reason, count(*) FROM quarantined_reading GROUP BY 1, 2;
SELECT record->>'sensor_name' AS sensor, count(*) FROM quarantined_reading
WHERE check_name = 'known_sensor' GROUP BY 1;
```

Replaying quarantined rows after the cause is fixed (for example, the sensor is added to the
registry) is not built yet. They keep their file and line, so it would be a re-merge of
those records through the same checks.

The sensor name is used as a natural key, which keeps the generic loader simple: the CSV value
is the foreign key, so no lookup step is needed. At billions of rows, an integer `sensor_id`
would make readings smaller and faster; that is what I would change for production.

## Design notes

- **Streaming.** Each worker reads its file row by row and flushes batches into a
  temporary staging table with `COPY`, so memory stays flat regardless of file size.
- **Batch commits with a checkpoint.** A file is loaded in batches of
  `processing.batch_size` rows (default 10,000; override with `BATCH_SIZE`). Each batch
  is one transaction: its rows, its rejected, quarantined and conflict rows, the file's
  counters, and the checkpoint `ingest_file.committed_line_no` all commit together. So a
  batch is applied exactly once. A failure or crash keeps the batches committed so far, and
  the next run resumes after the checkpoint. The counters always match what is in the
  database, and unique `(file, line)` constraints on the audit tables guard against
  anything being recorded twice.

  I chose this over one transaction per file. A long transaction holds its keys locked
  until it commits, so other workers with the same keys wait. It also stops Postgres from
  cleaning up old row versions while it is open, and a late failure redoes the whole file.
  These costs grow with file size and worker count. The trade-off is that readers can
  see a file half-loaded while it is in progress (`status = 'processing'`), and a corrupt
  file keeps the rows before the corruption. Rows are independent readings, so that is
  acceptable here.

  Resume assumes the file has not changed since the failed attempt; see "Limitations".
- **Duplicates and conflicts are resolved in the database.** The primary key
  `(sensor_name, ts)` enforces one reading per sensor per instant. First row wins: the
  lowest line within a file, the first committed file across files. A losing row with
  different values is written to `ingest_conflict` with both values and where each came
  from; a losing row with the same values is only counted (`rows_duplicate`).
- **Conflicts are detected after the insert, not before.** With parallel workers, another
  file may hold an uncommitted row for the same key. The `INSERT … ON CONFLICT DO NOTHING`
  waits for it, and the conflict query that follows (a new READ COMMITTED snapshot) then
  sees it. Checking first would miss it. Across files running in parallel, which one is
  "first" depends on commit order.
- **Config-driven dataset.** Column mapping, types, rules, checks and the unique key come
  from YAML; parsers, rules and checks are small named registries in code.

## Parallelism

`processing.workers` (or `--workers`) worker processes each load whole files, with one
database connection per worker. They are processes, not threads, because parsing and
validating rows is CPU-bound Python, which the GIL would serialise. With 1 worker everything
runs in the main process, which is easier to debug.

Workers share nothing in Python; they coordinate only through the database:

- **A file is loaded by one worker at a time.** While a worker owns a file it holds a
  session-level advisory lock on the file's id (`pg_try_advisory_lock`). Another worker, or a
  second copy of the program started at the same time, sees the lock and skips the file.
- **Crash recovery needs no timeouts.** A worker that dies loses its connection, so Postgres
  releases its lock. Its file is left as `processing` and the next run picks it up, while a
  worker that is merely slow keeps its lock.
- **No deadlocks.** Two workers inserting the same keys lock them in key order (the merge
  inserts in key order), so neither can wait on the other in a cycle. A test loads the same
  keys in opposite orders from 4 workers.
- **Conflicts across workers are still logged**, because they are detected after the insert
  (see above).

**What parallel runs change:** across files, "first" means first committed, so with more
than one worker the winner of a key that appears in several files depends on timing. Rows
stored, rejected and quarantined are always the same. Only which of two differing values is
kept (and so whether a later copy counts as a duplicate or a conflict) can vary between runs.
On the mock data, 1 worker matches the manifest exactly. With 4 or 8 workers about 0.1% of
the duplicate/conflict rows swap categories, and their total is unchanged. If the earliest
file has to win regardless of timing, the merge would compare `(file, line)` against the kept
row and replace it when it is earlier. That is possible, but it makes the kept value of a key
change over time.

Timing on the mock data (1M rows, 10 files; 8-core Mac, Postgres in Docker with 2 CPUs):

| Workers | Time |
|---|---|
| 1 | 29.7 s |
| 4 | 21.1 s |
| 8 | 20.7 s |

Beyond about 4 workers the database, not Python, is the bottleneck here. More database CPUs
or a faster disk would move it.

Inspecting conflicts:

```sql
SELECT unique_key, kept_values, rejected_values, f.file_name, rejected_line_no
FROM ingest_conflict c JOIN ingest_file f ON f.id = c.rejected_file_id;
```

## Assumptions

- Timestamps must be a full ISO 8601 date-time (`YYYY-MM-DDTHH:MM[:SS[.ffffff]]`) with an
  offset (`Z` or `±HH:MM`); date-only, space-separated or naive timestamps are rejected.
  They are stored in UTC, so `10:00Z` and `18:00+08:00` are the same reading.
- Values must be plain decimal or scientific notation; `NaN`, `Infinity`, `true`, `1_000`
  and out-of-range numbers are rejected.
- Fields are trimmed of surrounding whitespace; an empty sensor name is rejected.
- Blank lines are skipped. A row with the wrong number of fields is rejected.
- The header must contain the configured columns (any order, extra columns allowed);
  otherwise the whole file is marked `failed`. So is a file that is not valid in the
  configured encoding.
- A file whose status is `done` is skipped on rerun; `failed` files are retried.

## Limitations and next steps

- **Replaying quarantined rows** once their cause is fixed (for example a sensor added to the
  registry) is not built.
- **File identity is the file name.** That way the same input directory is recognised
  whether it is read on the host or mounted in Docker at another path. It assumes names are
  never reused: a new file with an old name is skipped as already loaded, and a failed file
  fixed in place resumes by line number, which is only right if the lines before the
  checkpoint did not change. If names could repeat, a content hash per file would be the
  identity instead, at the cost of one extra read of each file.
- **Scale:** at billions of rows, use an integer `sensor_id` key and partition
  `sensor_reading` by time (or use TimescaleDB). Bulk backfills would benefit from loading
  first and building indexes afterwards.
- **Operations:** metrics and alerts on rejection, quarantine and conflict rates per file
  and per sensor.

## Mock data

`make generate-data` writes 10 × 100,000-row files plus `manifest.json`, which lists exactly
how many bad rows (by kind), duplicates and conflicts were injected, within and across files.
Readings come from the sensors in `config/reference`, with realistic values for each type
(for example °C 18–30, CO2 400–1500 ppm) that drift gradually per sensor. Small shares of
rows come from three sensors that are not in the registry, or carry impossible values
(for example 120 °C), to exercise quarantine. Tune it with
`python scripts/generate_data.py --help`. On the default data set with 1 worker, the counts in
the database (rows, `rejected_record`, `quarantined_reading` per check, `rows_duplicate`,
`ingest_conflict`) match the manifest exactly. With several workers, see "What parallel runs
change" above.
