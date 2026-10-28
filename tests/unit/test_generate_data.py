import importlib.util
import json
import sys
from pathlib import Path

import pytest

from ingestor.reader import open_csv
from ingestor.validation import RecordValidator, Rejection
from tests.conftest import SENSOR_COLUMNS

SCRIPT = Path(__file__).parents[2] / "scripts" / "generate_data.py"


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_data", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["generate_data"] = module   # dataclasses need the module registered
    spec.loader.exec_module(module)
    return module


gen = load_generator()


def generate(out: Path, **overrides) -> dict:
    argv = [
        "--out", str(out), "--files", "3", "--rows", "4000", "--sensors", "5",
        "--bad-ratio", "0.03", "--dup-ratio", "0.03", "--conflict-ratio", "0.02",
    ]
    for key, value in overrides.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    return gen.generate(gen.build_parser().parse_args(argv))


def classify(out: Path) -> dict:
    """Validate every row with the project validator and sort rows into kinds."""
    seen: dict[tuple, float] = {}
    result = {"valid_unique": 0, "duplicate": 0, "conflict": 0, "rejected": 0, "blank": 0}
    for path in sorted(out.glob("sensors-*.csv")):
        result["blank"] += sum(1 for line in path.read_text().split("\n")[1:-1] if line == "")
        with open_csv(path) as source:
            validator = RecordValidator(SENSOR_COLUMNS, source.header)
            for row in source.rows():
                record = validator.validate(row)
                if isinstance(record, Rejection):
                    result["rejected"] += 1
                    continue
                key = (record.values["sensor_name"], record.values["ts"])
                if key not in seen:
                    seen[key] = record.values["value"]
                    result["valid_unique"] += 1
                elif seen[key] == record.values["value"]:
                    result["duplicate"] += 1
                else:
                    result["conflict"] += 1
    return result


def test_header_and_file_names(tmp_path):
    generate(tmp_path)
    names = sorted(p.name for p in tmp_path.glob("*.csv"))
    assert names == ["sensors-0001.csv", "sensors-0002.csv", "sensors-0003.csv"]
    for name in names:
        assert (tmp_path / name).read_text().split("\n", 1)[0] == "timestamp,sensorName,value"


def test_deterministic_for_same_seed(tmp_path):
    generate(tmp_path / "a", seed=7)
    generate(tmp_path / "b", seed=7)
    generate(tmp_path / "c", seed=8)
    for name in ("sensors-0001.csv", "sensors-0003.csv"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
    assert (tmp_path / "a/sensors-0001.csv").read_bytes() != (
        tmp_path / "c/sensors-0001.csv"
    ).read_bytes()


def test_manifest_matches_files(tmp_path):
    manifest = generate(tmp_path)
    assert json.loads((tmp_path / "manifest.json").read_text()) == manifest
    total = manifest["total"]
    found = classify(tmp_path)

    assert total["valid_unique"] == found["valid_unique"] == manifest["expected_db_rows"]
    assert total["bad"]["total"] == found["rejected"]
    assert total["duplicate"] == found["duplicate"]
    assert total["conflict"] == found["conflict"]
    assert total["blank"] == found["blank"]
    assert total["rows"] == 3 * 4000
    assert sum(f["valid_unique"] + f["bad"]["total"] + f["duplicate"] + f["conflict"]
               for f in manifest["files"]) == total["rows"]


def test_all_injected_kinds_appear(tmp_path):
    total = generate(tmp_path, rows=20_000)["total"]
    assert set(total["bad"]) == {"total", *gen.BAD_KINDS}
    assert total["duplicate_cross_file"] > 0
    assert total["conflict_cross_file"] > 0
    assert total["duplicate"] > total["duplicate_cross_file"]
    assert total["blank"] > 0


def test_cross_file_ratio_zero_keeps_duplicates_in_file(tmp_path):
    manifest = generate(tmp_path, cross_file_ratio=0)
    assert manifest["total"]["duplicate_cross_file"] == 0
    assert manifest["total"]["conflict_cross_file"] == 0
    # Each file on its own already resolves every duplicate/conflict to an earlier row.
    for entry in manifest["files"]:
        alone = tmp_path / "alone"
        alone.mkdir(exist_ok=True)
        (alone / entry["file"]).write_bytes((tmp_path / entry["file"]).read_bytes())
        found = classify(alone)
        assert found["duplicate"] == entry["duplicate"]
        assert found["conflict"] == entry["conflict"]
        (alone / entry["file"]).unlink()


def test_valid_rows_use_varied_offsets_and_number_formats(tmp_path):
    generate(tmp_path, rows=5000)
    text = (tmp_path / "sensors-0001.csv").read_text()
    for suffix in ("Z,", "+00:00,", "+08:00,", "-05:00,"):
        assert suffix in text
    assert "e+" in text or "e-" in text


def test_zero_ratios_give_only_unique_valid_rows(tmp_path):
    manifest = generate(
        tmp_path, bad_ratio=0, dup_ratio=0, conflict_ratio=0, files=2, rows=500
    )
    total = manifest["total"]
    assert total["valid_unique"] == 1000
    assert total["bad"]["total"] == total["duplicate"] == total["conflict"] == 0
    found = classify(tmp_path)
    assert found["valid_unique"] == 1000 and found["rejected"] == 0


def test_start_without_offset_is_rejected(tmp_path):
    with pytest.raises(SystemExit):
        generate(tmp_path, start="2024-01-01T00:00:00")
