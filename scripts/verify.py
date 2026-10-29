"""Check the database against the mock-data manifest after a load.

    python scripts/verify.py [--manifest data/input/manifest.json] [--db-url URL]

Every count must match what the generator injected. The one exception: with more
than one worker, which of two differing values for a key is kept depends on commit
order, so duplicates and conflicts are compared as a total (see README, "What
parallel runs change"). Exits 1 if anything does not match.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg

DEFAULT_URL = "postgresql://ingestor:ingestor@localhost:5432/sensors"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path, default=Path("data/input/manifest.json"))
    parser.add_argument("--db-url", default=os.environ.get("DATABASE_URL", DEFAULT_URL))
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text())
    expected = manifest["total"]

    with psycopg.connect(args.db_url) as conn:
        def one(query: str) -> int:
            return conn.execute(query).fetchone()[0] or 0

        files = dict(conn.execute("SELECT status, count(*) FROM ingest_file GROUP BY 1").fetchall())
        # sum() of bigint is numeric in Postgres; cast back so it prints as an integer.
        counters = conn.execute(
            "SELECT sum(rows_inserted)::bigint, sum(rows_rejected)::bigint,"
            " sum(rows_quarantined)::bigint, sum(rows_duplicate)::bigint,"
            " sum(rows_conflict)::bigint FROM ingest_file"
        ).fetchone()
        stored = {
            "readings": one("SELECT count(*) FROM sensor_reading"),
            "rejected": one("SELECT count(*) FROM rejected_record"),
            "quarantined": one("SELECT count(*) FROM quarantined_reading"),
            "conflicts": one("SELECT count(*) FROM ingest_conflict"),
        }
        unknown = one("SELECT count(*) FROM quarantined_reading WHERE check_name = 'known_sensor'")
        out_of_range = one(
            "SELECT count(*) FROM quarantined_reading WHERE check_name = 'value_in_type_range'"
        )

    duplicate = counters[3] or 0
    checks = [
        ("files done", files.get("done", 0), len(manifest["files"])),
        ("readings stored", stored["readings"], expected["valid_unique"]),
        ("rows rejected", stored["rejected"], expected["bad"]["total"]),
        ("quarantined: unknown sensor", unknown, expected["quarantined"]["unknown_sensor"]),
        ("quarantined: out of range", out_of_range, expected["quarantined"]["out_of_range"]),
        ("duplicates + conflicts", duplicate + stored["conflicts"],
         expected["duplicate"] + expected["conflict"]),
        # The per-file counters must agree with what the tables actually hold.
        ("counters vs tables", tuple(c or 0 for c in counters[:3]) + (counters[4] or 0,),
         (stored["readings"], stored["rejected"], stored["quarantined"], stored["conflicts"])),
    ]

    failed = 0
    for name, got, want in checks:
        ok = got == want
        failed += not ok
        print(f"{'OK  ' if ok else 'FAIL'}  {name:30} database={got}  expected={want}")
    print(f"info  {'duplicates / conflicts':30} database={duplicate} / {stored['conflicts']}  "
          f"manifest={expected['duplicate']} / {expected['conflict']}  (split depends on workers)")
    not_done = [f"{n} {status}" for status, n in files.items() if status != "done"]
    if not_done:
        print(f"note  files not done: {', '.join(not_done)}")

    print("\nAll checks passed." if not failed else f"\n{failed} check(s) failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
