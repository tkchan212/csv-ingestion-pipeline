"""Generate mock sensor CSV files, with a controllable share of bad and duplicate rows.

Rows are streamed to disk. Duplicates and conflicts copy the key of an earlier valid
row picked from a bounded reservoir sample, so memory stays flat however big the files get.
A manifest.json next to the files records exactly what was injected.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

HEADER = "timestamp,sensorName,value"
POOL_SIZE = 10_000          # max remembered keys per pool (current file / earlier files)
FLUSH_ROWS = 10_000
BLANK_RATE = 0.0002         # blank lines per data row (not counted in --rows)
OFFSETS = (("Z", 0), ("+00:00", 0), ("+08:00", 8 * 3600), ("-05:00", -5 * 3600))
BAD_KINDS = (
    "naive_timestamp",
    "date_only",
    "garbage_timestamp",
    "space_separator",
    "non_numeric_value",
    "empty_sensor",
    "missing_field",
    "extra_field",
)
GARBAGE_TIMESTAMPS = ("yesterday", "01/03/2024 10:15", "not-a-date", "2024-13-45T99:99:99Z")
NON_NUMERIC = ("true", "ABC", "NaN", "inf", "")
NOTE = (
    "Counts are what the generator injected. Valid base rows have globally unique "
    "(sensor, instant) keys and each key is the target of at most one conflict, so "
    "the DB should hold valid_unique rows; it may differ slightly only if the same "
    "key is picked as a duplicate target both before and after a conflict."
)

# A remembered valid row: (sensor, UTC epoch seconds, value as written).
Key = tuple[str, int, str]


@dataclass
class Pool:
    """Bounded uniform sample (reservoir) of valid rows seen so far."""

    items: list[Key] = field(default_factory=list)
    seen: int = 0

    def add(self, item: Key, rng: random.Random) -> None:
        self.seen += 1
        if len(self.items) < POOL_SIZE:
            self.items.append(item)
        elif (j := rng.randrange(self.seen)) < POOL_SIZE:
            self.items[j] = item

    def pick(self, rng: random.Random) -> Key:
        return self.items[rng.randrange(len(self.items))]

    def take(self, rng: random.Random) -> Key:
        """Remove and return a random item."""
        i = rng.randrange(len(self.items))
        self.items[i], self.items[-1] = self.items[-1], self.items[i]
        return self.items.pop()


def format_timestamp(epoch: int, offset: tuple[str, int]) -> str:
    """Write a UTC epoch as ISO 8601 with the given offset suffix."""
    suffix, seconds = offset
    return datetime.fromtimestamp(epoch + seconds, UTC).isoformat()[:19] + suffix


def random_value(rng: random.Random) -> str:
    """A plain int, decimal, negative decimal or (rarely) scientific-notation number."""
    r = rng.random()
    if r < 0.15:
        return str(rng.randint(-100, 1000))
    if r < 0.90:
        return f"{rng.uniform(-50, 150):.{rng.choice((1, 2, 3))}f}"
    return f"{rng.uniform(-9, 9) * 10 ** rng.randint(-6, 6):.3e}"


def different_value(old: str, rng: random.Random) -> str:
    """A value that is numerically different from `old`."""
    while (new := random_value(rng)) and float(new) == float(old):
        pass
    return new


def bad_row(kind: str, ts: str, sensor: str, value: str, rng: random.Random) -> str:
    """Corrupt one valid row in the way `kind` says."""
    match kind:
        case "naive_timestamp":
            return f"{ts[:19]},{sensor},{value}"
        case "date_only":
            return f"{ts[:10]},{sensor},{value}"
        case "garbage_timestamp":
            return f"{rng.choice(GARBAGE_TIMESTAMPS)},{sensor},{value}"
        case "space_separator":
            return f"{ts[:10]} {ts[11:]},{sensor},{value}"
        case "non_numeric_value":
            return f"{ts},{sensor},{rng.choice(NON_NUMERIC)}"
        case "empty_sensor":
            return f"{ts},,{value}"
        case "missing_field":
            return f"{ts},{sensor}"
        case _:
            return f"{ts},{sensor},{value},extra"


class Generator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.rng = random.Random(args.seed)
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        if start.tzinfo is None:
            raise SystemExit("--start needs a time zone offset, e.g. 2024-01-01T00:00:00Z")
        self.start_epoch = int(start.timestamp())
        self.sensors = [f"temp-{n:04d}" for n in range(args.sensors)]
        self.earlier = Pool()       # sample of valid rows from finished files

    def write_file(self, path: Path, file_index: int) -> dict:
        """Write one CSV file and return what was injected."""
        a, rng = self.args, self.rng
        counts: Counter[str] = Counter()
        current = Pool()
        buf: list[str] = [HEADER]
        bad_cut = a.bad_ratio
        dup_cut = bad_cut + a.dup_ratio
        conflict_cut = dup_cut + a.conflict_ratio
        with path.open("w", encoding="utf-8", newline="") as f:
            for n in range(a.rows):
                # One valid instant per row slot across all files: base keys never collide.
                epoch = self.start_epoch + file_index * a.rows + n
                offset = rng.choice(OFFSETS)
                r = rng.random()
                if r < bad_cut:
                    kind = rng.choice(BAD_KINDS)
                    sensor, value = rng.choice(self.sensors), random_value(rng)
                    ts = format_timestamp(epoch, offset)
                    buf.append(bad_row(kind, ts, sensor, value, rng))
                    counts[f"bad_{kind}"] += 1
                elif r < conflict_cut and (source := self._source(current)) is not None:
                    pool, cross = source
                    is_dup = r < dup_cut
                    sensor, ref_epoch, old = pool.pick(rng) if is_dup else pool.take(rng)
                    value = old if is_dup else different_value(old, rng)
                    buf.append(f"{format_timestamp(ref_epoch, offset)},{sensor},{value}")
                    label = "duplicate" if is_dup else "conflict"
                    counts[label] += 1
                    counts[f"{label}_cross_file"] += cross
                else:
                    sensor, value = rng.choice(self.sensors), random_value(rng)
                    buf.append(f"{format_timestamp(epoch, offset)},{sensor},{value}")
                    current.add((sensor, epoch, value), rng)
                    counts["valid_unique"] += 1
                if rng.random() < BLANK_RATE:
                    buf.append("")
                    counts["blank"] += 1
                if len(buf) >= FLUSH_ROWS:
                    f.write("\n".join(buf) + "\n")
                    buf.clear()
            if buf:
                f.write("\n".join(buf) + "\n")
        self._merge(current)
        return summarize(path.name, a.rows, counts)

    def _source(self, current: Pool) -> tuple[Pool, bool] | None:
        """Pick the pool a duplicate/conflict copies from; None if nothing is available."""
        want_cross = self.rng.random() < self.args.cross_file_ratio
        for cross in (want_cross, not want_cross):
            pool = self.earlier if cross else current
            if pool.items:
                return pool, cross
        return None

    def _merge(self, current: Pool) -> None:
        """Fold the finished file's sample into the bounded earlier-files sample."""
        merged = self.earlier.items + current.items
        if len(merged) > POOL_SIZE:
            merged = self.rng.sample(merged, POOL_SIZE)
        self.earlier = Pool(merged, self.earlier.seen + current.seen)


def summarize(name: str, rows: int, counts: Counter[str]) -> dict:
    bad = {k.removeprefix("bad_"): v for k, v in sorted(counts.items()) if k.startswith("bad_")}
    return {
        "file": name,
        "rows": rows,
        "valid_unique": counts["valid_unique"],
        "bad": {"total": sum(bad.values()), **bad},
        "duplicate": counts["duplicate"],
        "duplicate_cross_file": counts["duplicate_cross_file"],
        "conflict": counts["conflict"],
        "conflict_cross_file": counts["conflict_cross_file"],
        "blank": counts["blank"],
    }


def total(files: list[dict]) -> dict:
    out: dict = {"file": "total"}
    for key in ("rows", "valid_unique", "duplicate", "duplicate_cross_file",
                "conflict", "conflict_cross_file", "blank"):
        out[key] = sum(f[key] for f in files)
    bad: Counter[str] = Counter()
    for f in files:
        bad.update(f["bad"])
    out["bad"] = dict(sorted(bad.items()))
    return out


def generate(args: argparse.Namespace) -> dict:
    """Write the CSV files and manifest.json under args.out; return the manifest."""
    args.out.mkdir(parents=True, exist_ok=True)
    gen = Generator(args)
    files = [
        gen.write_file(args.out / f"sensors-{i + 1:04d}.csv", i) for i in range(args.files)
    ]
    totals = total(files)
    manifest = {
        "seed": args.seed,
        "args": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "note": NOTE,
        "expected_db_rows": totals["valid_unique"],
        "files": files,
        "total": totals,
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("data/input"))
    parser.add_argument("--files", type=int, default=10)
    parser.add_argument("--rows", type=int, default=100_000, help="rows per file (excl. blanks)")
    parser.add_argument("--bad-ratio", type=float, default=0.01)
    parser.add_argument("--dup-ratio", type=float, default=0.01)
    parser.add_argument("--conflict-ratio", type=float, default=0.005)
    parser.add_argument(
        "--cross-file-ratio", type=float, default=0.3,
        help="share of duplicates/conflicts copied from an earlier file",
    )
    parser.add_argument("--sensors", type=int, default=50)
    parser.add_argument("--start", default="2024-01-01T00:00:00Z", help="ISO 8601 start time")
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    t = generate(args)["total"]
    print(
        f"Wrote {args.files} file(s) x {args.rows} rows to {args.out}: "
        f"valid_unique={t['valid_unique']} bad={t['bad'].get('total', 0)} "
        f"duplicate={t['duplicate']} conflict={t['conflict']} blank={t['blank']}"
    )


if __name__ == "__main__":
    main()
