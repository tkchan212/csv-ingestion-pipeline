"""Generate mock sensor CSV files, with a controllable share of bad and duplicate rows.

Rows are streamed to disk. Duplicates and conflicts copy the key of an earlier valid
row picked from a bounded reservoir sample, so memory stays flat however big the files get.
A manifest.json next to the files records exactly what was injected.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
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

# Mock-data knowledge per sensor type: (low, high, decimals, random-walk step, glitch size).
TYPE_PROFILES = {
    "TMP": (18.0, 30.0, 1, 0.2, (0.5, 3.0)),
    "HUM": (30.0, 70.0, 1, 0.8, (1.0, 5.0)),
    "CO2": (400.0, 1500.0, 0, 25.0, (10.0, 80.0)),
    "PWR": (2.0, 120.0, 2, 2.0, (0.5, 8.0)),
}
# Not in the registry, so the known_sensor check quarantines them.
UNKNOWN_SENSORS = {"HQT-TMP-13-01": "TMP", "RSA-HUM-05-01": "HUM", "DC1-PWR-02-01": "PWR"}
# Intervals (low, high) outside each type's valid range, for implausible readings.
IMPLAUSIBLE = {
    "TMP": ((85.0, 150.0), (-60.0, -21.0)),
    "HUM": ((101.0, 150.0),),
    "CO2": ((-500.0, -1.0), (6000.0, 20000.0)),
    "PWR": ((-50.0, -1.0), (600.0, 2000.0)),
}
SCI_RATE = 0.02             # share of values written in scientific notation
NOTE = (
    "valid_unique rows should end up in sensor_reading; quarantined rows (unknown sensor "
    "or value outside the type's range) are well-formed but should go to the quarantine "
    "table. Base rows have globally unique (sensor, instant) keys, only they feed "
    "duplicates and conflicts, and each key is the target of at most one conflict, so "
    "counts should be exact; they may differ slightly only if the same key is picked as "
    "a duplicate target both before and after a conflict."
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


def load_bounds(reference_dir: Path) -> dict[str, tuple[float, float]]:
    """Read {type code: (valid_min, valid_max)} from sensor_types.csv."""
    try:
        with (reference_dir / "sensor_types.csv").open(encoding="utf-8", newline="") as f:
            bounds = {
                row["code"]: (float(row["valid_min"]), float(row["valid_max"]))
                for row in csv.DictReader(f)
            }
    except (OSError, KeyError, ValueError) as exc:
        raise SystemExit(f"cannot read sensor types: {exc!r}") from None
    for code, (low, high, *_) in TYPE_PROFILES.items():
        if code not in bounds or not bounds[code][0] <= low < high <= bounds[code][1]:
            raise SystemExit(f"type {code}: generated range is not inside its valid range")
    return bounds


def implausible_value(
    type_code: str, bounds: tuple[float, float], rng: random.Random
) -> str:
    """A well-formed reading outside the type's valid range."""
    decimals = TYPE_PROFILES[type_code][2]
    outside = [(lo, hi) for lo, hi in IMPLAUSIBLE[type_code] if hi < bounds[0] or lo > bounds[1]]
    if not outside:
        raise SystemExit(f"type {type_code}: no implausible range outside {bounds}")
    low, high = rng.choice(outside)
    return render_value(round(rng.uniform(low, high), decimals), decimals, rng)


def load_sensors(reference_dir: Path) -> dict[str, str]:
    """Read sensors.csv from the reference dir into {sensor name: type code}."""
    try:
        with (reference_dir / "sensors.csv").open(encoding="utf-8", newline="") as f:
            sensors = {row["name"]: row["type_code"] for row in csv.DictReader(f)}
    except OSError as exc:
        raise SystemExit(f"cannot read sensor registry: {exc}") from None
    unsupported = sorted(set(sensors.values()) - TYPE_PROFILES.keys())
    if not sensors or unsupported:
        raise SystemExit(f"sensors.csv is empty or has unsupported type(s): {unsupported}")
    return sensors


def clamp(profile: tuple, value: float) -> float:
    low, high, decimals, *_ = profile
    return round(min(max(value, low), high), decimals)


def render_value(value: float, decimals: int, rng: random.Random) -> str:
    """Write a reading as plain decimal, or occasionally with a '+', '.0' or exponent."""
    text = f"{value:.{decimals}f}"
    r = rng.random()
    if r < SCI_RATE:
        return format(Decimal(text), "e").lower()
    if r < SCI_RATE + 0.01:
        return text if text.startswith("-") else "+" + text
    if decimals == 0 and r < SCI_RATE + 0.06:
        return text + ".0"
    return text


def next_value(state: dict[str, float], sensor: str, type_code: str, rng: random.Random) -> str:
    """Advance the sensor's random walk one step and return the reading as text."""
    profile = TYPE_PROFILES[type_code]
    low, high, decimals, step, _ = profile
    current = state.get(sensor)
    value = rng.uniform(low, high) if current is None else current + rng.gauss(0, step)
    state[sensor] = clamp(profile, value)
    return render_value(state[sensor], decimals, rng)


def glitch_value(old: str, type_code: str, rng: random.Random) -> str:
    """The kept value plus a small non-zero delta, numerically different once written."""
    profile = TYPE_PROFILES[type_code]
    low, high, decimals, _, (dmin, dmax) = profile
    old_number = float(old)
    while True:
        new = clamp(profile, old_number + rng.choice((-1, 1)) * rng.uniform(dmin, dmax))
        if new != round(old_number, decimals):
            return render_value(new, decimals, rng)


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
        self.bounds = load_bounds(args.reference_dir)
        self.types = load_sensors(args.reference_dir)
        self.sensors = list(self.types)
        self.unknown = (
            {n: t for n, t in UNKNOWN_SENSORS.items() if n not in self.types}
            if args.unknown_sensor_ratio > 0
            else {}
        )
        self.types |= self.unknown
        self.state: dict[str, float] = {}     # current reading per sensor (random walk)
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
                    sensor = rng.choice(self.sensors)
                    value = self._value(sensor)
                    ts = format_timestamp(epoch, offset)
                    buf.append(bad_row(kind, ts, sensor, value, rng))
                    counts[f"bad_{kind}"] += 1
                elif r < conflict_cut and (source := self._source(current)) is not None:
                    pool, cross = source
                    is_dup = r < dup_cut
                    sensor, ref_epoch, old = pool.pick(rng) if is_dup else pool.take(rng)
                    value = old if is_dup else glitch_value(old, self.types[sensor], rng)
                    buf.append(f"{format_timestamp(ref_epoch, offset)},{sensor},{value}")
                    label = "duplicate" if is_dup else "conflict"
                    counts[label] += 1
                    counts[f"{label}_cross_file"] += cross
                else:
                    buf.append(self._fresh_row(epoch, offset, current, counts))
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

    def _fresh_row(
        self, epoch: int, offset: tuple[str, int], current: Pool, counts: Counter[str]
    ) -> str:
        """A well-formed row at a new instant: loadable, unknown-sensor or out-of-range."""
        a, rng = self.args, self.rng
        q = rng.random()
        if self.unknown and q < a.unknown_sensor_ratio:
            sensor = rng.choice(list(self.unknown))
            value = self._value(sensor)
            counts["quarantined_unknown_sensor"] += 1
        elif q < a.unknown_sensor_ratio + a.implausible_ratio:
            sensor = rng.choice(self.sensors)
            type_code = self.types[sensor]
            value = implausible_value(type_code, self.bounds[type_code], rng)
            counts["quarantined_out_of_range"] += 1
        else:
            sensor = rng.choice(self.sensors)
            value = self._value(sensor)
            current.add((sensor, epoch, value), rng)     # only loadable rows feed the pools
            counts["valid_unique"] += 1
        return f"{format_timestamp(epoch, offset)},{sensor},{value}"

    def _value(self, sensor: str) -> str:
        return next_value(self.state, sensor, self.types[sensor], self.rng)

    def _source(self, current: Pool) -> tuple[Pool, bool] | None:
        """Pick the pool a duplicate/conflict copies from; None if nothing is available."""
        want_cross = self.rng.random() < self.args.cross_file_ratio
        if want_cross and self.earlier.items:
            return self.earlier, True
        return (current, False) if current.items else None

    def _merge(self, current: Pool) -> None:
        """Fold the finished file's sample into the bounded earlier-files sample."""
        merged = self.earlier.items + current.items
        if len(merged) > POOL_SIZE:
            merged = self.rng.sample(merged, POOL_SIZE)
        self.earlier = Pool(merged, self.earlier.seen + current.seen)


def summarize(name: str, rows: int, counts: Counter[str]) -> dict:
    quarantined = {
        "unknown_sensor": counts["quarantined_unknown_sensor"],
        "out_of_range": counts["quarantined_out_of_range"],
    }
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
        "quarantined": {
            "total": sum(quarantined.values()),
            **quarantined,
        },
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
    out["quarantined"] = {
        k: sum(f["quarantined"][k] for f in files)
        for k in ("total", "unknown_sensor", "out_of_range")
    }
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
        "unknown_sensors": list(gen.unknown),
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
    parser.add_argument("--reference-dir", type=Path, default=Path("config/reference"))
    parser.add_argument(
        "--unknown-sensor-ratio", type=float, default=0.001,
        help="share of rows from sensors missing from the registry (quarantined)",
    )
    parser.add_argument(
        "--implausible-ratio", type=float, default=0.002,
        help="share of rows with a value outside the sensor type's range (quarantined)",
    )
    parser.add_argument("--start", default="2024-01-01T00:00:00Z", help="ISO 8601 start time")
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    t = generate(args)["total"]
    print(
        f"Wrote {args.files} file(s) x {args.rows} rows to {args.out}: "
        f"valid_unique={t['valid_unique']} bad={t['bad'].get('total', 0)} "
        f"quarantined={t['quarantined']['total']} duplicate={t['duplicate']} "
        f"conflict={t['conflict']} blank={t['blank']}"
    )


if __name__ == "__main__":
    main()
