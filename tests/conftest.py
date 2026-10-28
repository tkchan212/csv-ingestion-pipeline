from pathlib import Path

import pytest

from ingestor.config import (
    ColumnConfig,
    DatasetConfig,
    PipelineConfig,
    ProcessingConfig,
    SourceConfig,
    TargetConfig,
)

# Mirrors config/pipeline.yaml until YAML loading exists.
SENSOR_COLUMNS = (
    ColumnConfig(source="timestamp", target="ts", type="timestamp_tz"),
    ColumnConfig(source="sensorName", target="sensor_name", type="string", rules=("non_empty",)),
    ColumnConfig(source="value", target="value", type="float"),
)
SENSOR_DATASET = DatasetConfig(columns=SENSOR_COLUMNS, unique_key=("sensor_name", "ts"))


@pytest.fixture
def write_csv(tmp_path: Path):
    """Write a CSV file from a list of lines and return its path."""

    def write(name: str, lines: list[str]) -> Path:
        path = tmp_path / name
        path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
        return path

    return write


def make_config(input_dir: Path, database_url: str, batch_size: int = 10_000) -> PipelineConfig:
    return PipelineConfig(
        source=SourceConfig(directory=input_dir),
        target=TargetConfig(database_url=database_url, table="sensor_reading"),
        dataset=SENSOR_DATASET,
        processing=ProcessingConfig(workers=1, batch_size=batch_size),
    )
