"""Typed pipeline configuration, loaded from YAML with ${ENV} expansion."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SourceConfig:
    directory: Path
    pattern: str = "*.csv"
    encoding: str = "utf-8"
    delimiter: str = ","


@dataclass(frozen=True)
class TargetConfig:
    database_url: str
    table: str


@dataclass(frozen=True)
class ProcessingConfig:
    workers: int = 4
    batch_size: int = 10_000


@dataclass(frozen=True)
class ColumnConfig:
    source: str
    target: str
    type: str
    rules: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatasetConfig:
    columns: tuple[ColumnConfig, ...]
    unique_key: tuple[str, ...]
    on_conflict: str = "first_wins"


@dataclass(frozen=True)
class PipelineConfig:
    source: SourceConfig
    target: TargetConfig
    dataset: DatasetConfig
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)


def load_config(path: Path, overrides: dict[str, object] | None = None) -> PipelineConfig:
    """Read the YAML file, expand environment variables, apply CLI overrides, validate."""
    raise NotImplementedError
