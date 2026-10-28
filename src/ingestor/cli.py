"""Command-line entry point: parse flags, load config, run the pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path


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
    # Wired up to pipeline.run once config loading (step 3) exists.
    raise NotImplementedError("pipeline not implemented yet")
