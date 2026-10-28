"""Command-line entry point: parse flags, load config, run the pipeline.

Exit codes: 0 every file loaded or skipped, 1 at least one file failed,
2 the run could not start (bad config, missing directory, database unreachable).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import psycopg

from ingestor.config import ConfigError, load_config
from ingestor.pipeline import run

log = logging.getLogger("ingestor")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csv-ingestor",
        description="Load every CSV file in a directory into a PostgreSQL table.",
    )
    parser.add_argument("--config", type=Path, default=Path("config/pipeline.yaml"))
    parser.add_argument("--input-dir", type=Path, help="overrides source.directory")
    parser.add_argument("--db-url", help="overrides target.database_url")
    parser.add_argument("--workers", type=int, help="overrides processing.workers")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(processName)s %(name)s: %(message)s",
    )
    overrides = {
        key: value
        for key, value in {
            "source.directory": args.input_dir and str(args.input_dir),
            "target.database_url": args.db_url,
            "processing.workers": args.workers,
        }.items()
        if value is not None
    }

    try:
        config = load_config(args.config, overrides)
    except ConfigError as exc:
        log.error("invalid configuration in %s:\n%s", args.config, exc)
        return 2
    except OSError as exc:
        log.error("cannot read configuration: %s", exc)
        return 2

    try:
        results = run(config)
    except (FileNotFoundError, NotADirectoryError) as exc:
        log.error("%s", exc)
        return 2
    except psycopg.OperationalError as exc:
        log.error("cannot connect to the database: %s", exc)
        return 2
    return 1 if any(r.status == "failed" for r in results) else 0
