from pathlib import Path

import pytest
import yaml

from ingestor.config import ConfigError, load_config
from tests.conftest import SENSOR_DATASET

SHIPPED = Path(__file__).parents[2] / "config" / "pipeline.yaml"

MINIMAL = {
    "source": {"directory": "/data/input"},
    "target": {"database_url": "postgresql://db/x", "table": "sensor_reading"},
    "dataset": {
        "columns": [{"source": "ts", "target": "ts", "type": "timestamp_tz"},
                    {"source": "v", "target": "v", "type": "float"}],
        "unique_key": ["ts"],
    },
}


@pytest.fixture
def write_config(tmp_path):
    def write(data) -> Path:
        path = tmp_path / "pipeline.yaml"
        path.write_text(data if isinstance(data, str) else yaml.safe_dump(data))
        return path
    return write


def problems(path: Path, overrides=None) -> list[str]:
    with pytest.raises(ConfigError) as info:
        load_config(path, overrides)
    return info.value.problems


def with_changes(**sections) -> dict:
    data = yaml.safe_load(yaml.safe_dump(MINIMAL))     # deep copy
    for name, values in sections.items():
        data.setdefault(name, {}).update(values)
    return data


# --- the shipped file ----------------------------------------------------------

def test_shipped_config_loads_and_matches_the_test_dataset(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db/sensors")
    monkeypatch.delenv("INPUT_DIR", raising=False)

    config = load_config(SHIPPED)

    assert config.source.directory == Path("/data/input")
    assert config.target.database_url == "postgresql://u:p@db/sensors"
    assert config.processing.workers == 4
    assert config.dataset == SENSOR_DATASET


def test_shipped_config_without_database_url_names_the_variable(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert problems(SHIPPED) == [
        "target.database_url: environment variable DATABASE_URL is not set",
    ]


def test_cli_overrides_win_and_avoid_missing_variables(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    config = load_config(SHIPPED, {
        "target.database_url": "postgresql://cli/db",
        "source.directory": "/elsewhere",
        "processing.workers": 8,
    })

    assert config.target.database_url == "postgresql://cli/db"
    assert config.source.directory == Path("/elsewhere")
    assert config.processing.workers == 8


# --- environment expansion -------------------------------------------------------

def test_env_default_is_used_when_unset_or_empty(monkeypatch, write_config) -> None:
    monkeypatch.setenv("EMPTY", "")
    path = write_config(with_changes(
        source={"directory": "${NOPE_UNSET:-/default}", "pattern": "${EMPTY:-*.txt}"},
        processing={"workers": "${NOPE_UNSET:-3}"},
    ))

    config = load_config(path)

    assert config.source.directory == Path("/default")
    assert config.source.pattern == "*.txt"
    assert config.processing.workers == 3


def test_env_value_is_substituted_inside_strings(monkeypatch, write_config) -> None:
    monkeypatch.setenv("DB_HOST", "db.internal")
    path = write_config(with_changes(
        target={"database_url": "postgresql://u@${DB_HOST}:5432/x"},
    ))

    assert load_config(path).target.database_url == "postgresql://u@db.internal:5432/x"


# --- validation -----------------------------------------------------------------

def test_defaults_fill_optional_settings(write_config) -> None:
    config = load_config(write_config(MINIMAL))

    assert (config.source.pattern, config.source.encoding, config.source.delimiter) == (
        "*.csv", "utf-8", ","
    )
    assert (config.processing.workers, config.processing.batch_size) == (4, 10_000)
    assert config.dataset.on_conflict == "first_wins"


def test_all_problems_are_reported_together(write_config) -> None:
    data = with_changes(
        source={"encoding": "utf-99", "delimiter": ";;", "recurse": True},
        processing={"workers": 0, "batch_size": "lots"},
        dataset={"unique_key": ["ts", "sensor"], "on_conflict": "last_wins",
                 "checks": ["known_sensor", "sane"]},
    )
    data["dataset"]["columns"][1].update(type="money", rules=["positive"], unit="kW")

    assert sorted(problems(write_config(data))) == sorted([
        "source.recurse: unknown setting (expected one of: delimiter, directory, encoding,"
        " pattern)",
        "source.encoding: unknown encoding 'utf-99'",
        "source.delimiter: must be a single character",
        "processing.workers: must be a positive integer",
        "processing.batch_size: must be a positive integer",
        "dataset.columns[1].type: unknown type 'money' (known: string, timestamp_tz, float)",
        "dataset.columns[1].rules: unknown rule 'positive' (known: non_empty)",
        "dataset.columns[1].unit: unknown setting (expected one of: rules, source, target,"
        " type)",
        "dataset.checks: unknown check 'sane' (known: known_sensor, value_in_type_range)",
        "dataset.unique_key: 'sensor' is not a column target",
        "dataset.on_conflict: only 'first_wins' is supported",
    ])


def test_missing_sections_and_required_keys(write_config) -> None:
    path = write_config({"source": {}, "dataset": {"columns": [{"source": "a"}]}})

    assert sorted(problems(path)) == sorted([
        "source.directory: is required",
        "target: section is missing",
        "dataset.columns[0].target: is required",
        "dataset.columns[0].type: is required",
        "dataset.unique_key: is required",
    ])


def test_duplicate_column_names_are_rejected(write_config) -> None:
    data = with_changes()
    data["dataset"]["columns"].append({"source": "v", "target": "v", "type": "float"})

    assert problems(write_config(data)) == [
        "dataset.columns: source 'v' appears 2 times",
        "dataset.columns: target 'v' appears 2 times",
    ]


@pytest.mark.parametrize("text", ["source: [unclosed", "- just\n- a list\n"])
def test_file_that_is_not_a_yaml_mapping_is_rejected(write_config, text: str) -> None:
    assert len(problems(write_config(text))) == 1
