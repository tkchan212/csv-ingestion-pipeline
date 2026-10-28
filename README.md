# CSV Ingestion Pipeline

Loads every CSV file in a directory into PostgreSQL, in parallel, with row validation,
duplicate handling and conflict logging. What a record looks like (columns, types,
validation rules, unique key) is defined in [config/pipeline.yaml](config/pipeline.yaml).

> Status: steps 1–2 done (single-process load with conflict logging, tested).
> CLI is wired up in step 3.

## Quick start

```bash
make setup          # .env, local venv, PostgreSQL in Docker
make generate-data  # mock CSVs into ./data/input
make ingest         # run the ingestor container against ./data/input
make psql           # inspect results
```

Run with other settings by overriding the config:

```bash
docker compose run --rm ingestor --config /app/config/pipeline.yaml --workers 8
```

## Layout

```
config/pipeline.yaml      dataset + runtime configuration
db/migrations/            schema, applied by the Postgres container on first start
src/ingestor/
  cli.py                  flags -> config -> pipeline
  config.py               YAML loading, ${ENV} expansion, validation
  scanner.py              find input files
  reader.py               stream CSV rows (constant memory per file)
  validation.py           parsers + rules registries, row -> record | rejection
  writer.py               COPY into staging, merge into target, log conflicts
  pipeline.py             worker pool, per-file status and summary
scripts/                  setup.sh, generate_data.py
tests/unit, tests/integration
```

## Design notes

- **Streaming.** Each worker reads its file row by row and flushes batches into a
  temporary staging table with `COPY`, so memory stays flat regardless of file size.
- **One transaction per file.** The staging table is merged into the target in the same
  transaction, so a file is either fully loaded or not at all, and `ingest_file.status`
  makes reruns skip finished files.
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

Inspecting conflicts:

```sql
SELECT unique_key, kept_values, rejected_values, f.path, rejected_line_no
FROM ingest_conflict c JOIN ingest_file f ON f.id = c.rejected_file_id;
```
- **Config-driven dataset.** Column mapping, types, rules and the unique key come from
  YAML; parsers and rules are a small named registry in code.

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

## Work plan

1. ~~Streaming reader, validation and staging/merge writer (single process)~~ done
2. ~~Conflict detection and logging in the database~~ done
3. YAML config loading and the validation registry
4. Parallel worker pool (mock data generator and integration tests already done)

## Mock data

`make generate-data` writes 10 × 100,000-row files plus `manifest.json`, which lists exactly
how many bad rows (by kind), duplicates and conflicts were injected, within and across files.
Tune it with `python scripts/generate_data.py --help`. On the default data set, the counts in
the database (rows, `rejected_record`, `rows_duplicate`, `ingest_conflict`) match the manifest
exactly.
