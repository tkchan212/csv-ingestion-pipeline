"""Typed pipeline configuration, loaded from YAML with ${ENV} expansion.

Loading order: parse YAML, apply CLI overrides, expand ${VAR} and ${VAR:-default},
build the dataclasses, validate. Overrides go before expansion so that, for
example, --db-url works when DATABASE_URL is not set. Every problem found is
reported in one ConfigError instead of stopping at the first.
"""

from __future__ import annotations

import codecs
import os
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml


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
    checks: tuple[str, ...] = ()  # reference-data checks (checks.py); failures are quarantined


@dataclass(frozen=True)
class PipelineConfig:
    source: SourceConfig
    target: TargetConfig
    dataset: DatasetConfig
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)


class ConfigError(ValueError):
    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        super().__init__("\n".join(f"  - {p}" for p in problems))


_ENV_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_REQUIRED = object()


def load_config(path: Path, overrides: Mapping[str, object] | None = None) -> PipelineConfig:
    """Load `path`. `overrides` maps dotted keys such as "processing.workers" to values."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError([f"{path}: not valid YAML ({exc})"]) from None
    if not isinstance(raw, dict):
        raise ConfigError([f"{path}: must be a mapping of sections"])

    for dotted, value in (overrides or {}).items():
        section, key = dotted.split(".", 1)
        if isinstance(raw.setdefault(section, {}), dict):
            raw[section][key] = value

    problems: list[str] = []
    raw = _expand(raw, "", problems)
    config = _Builder(problems).pipeline(raw)
    if problems:
        raise ConfigError(problems)
    return config


def _expand(value: Any, path: str, problems: list[str]) -> Any:
    """Expand environment variables in every string value."""
    if isinstance(value, str):
        def substitute(match: re.Match[str]) -> str:
            name, default = match.groups()
            if env := os.environ.get(name):
                return env
            if default is not None:
                return default
            problems.append(f"{path}: environment variable {name} is not set")
            return ""
        return _ENV_VAR.sub(substitute, value)
    if isinstance(value, dict):
        return {k: _expand(v, f"{path}.{k}" if path else str(k), problems)
                for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v, f"{path}[{i}]", problems) for i, v in enumerate(value)]
    return value


class _Builder:
    """Turns the parsed YAML into dataclasses, recording problems as it goes."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems

    def pipeline(self, raw: dict) -> PipelineConfig:
        self._known_keys(raw, "", PipelineConfig)
        return PipelineConfig(
            source=self._section(raw, "source", _Builder.source),
            target=self._section(raw, "target", _Builder.target),
            dataset=self._section(raw, "dataset", _Builder.dataset),
            processing=self._section(raw, "processing", _Builder.processing, required=False),
        )

    def source(self, s: dict) -> SourceConfig:
        self._known_keys(s, "source", SourceConfig)
        config = SourceConfig(
            directory=Path(self._str(s, "source.directory")),
            pattern=self._str(s, "source.pattern", SourceConfig.pattern),
            encoding=self._str(s, "source.encoding", SourceConfig.encoding),
            delimiter=self._str(s, "source.delimiter", SourceConfig.delimiter),
        )
        try:
            codecs.lookup(config.encoding)
        except LookupError:
            self.problems.append(f"source.encoding: unknown encoding {config.encoding!r}")
        if len(config.delimiter) != 1:
            self.problems.append("source.delimiter: must be a single character")
        return config

    def target(self, s: dict) -> TargetConfig:
        self._known_keys(s, "target", TargetConfig)
        return TargetConfig(
            database_url=self._str(s, "target.database_url"),
            table=self._str(s, "target.table"),
        )

    def processing(self, s: dict) -> ProcessingConfig:
        self._known_keys(s, "processing", ProcessingConfig)
        return ProcessingConfig(
            workers=self._positive_int(s, "processing.workers", ProcessingConfig.workers),
            batch_size=self._positive_int(s, "processing.batch_size", ProcessingConfig.batch_size),
        )

    def dataset(self, s: dict) -> DatasetConfig:
        # Imported here: validation imports this module for ColumnConfig.
        from ingestor.checks import CHECKS
        from ingestor.validation import PARSERS, RULES

        self._known_keys(s, "dataset", DatasetConfig)
        raw_columns = self._get(s, "dataset.columns", _REQUIRED)
        if raw_columns is not _REQUIRED and not isinstance(raw_columns, list):
            self.problems.append("dataset.columns: must be a list")
        if not isinstance(raw_columns, list):
            raw_columns = []
        columns = []
        for i, c in enumerate(raw_columns):
            path = f"dataset.columns[{i}]"
            if not isinstance(c, dict):
                self.problems.append(f"{path}: must be a mapping")
                continue
            self._known_keys(c, path, ColumnConfig)
            column = ColumnConfig(
                source=self._str(c, f"{path}.source"),
                target=self._str(c, f"{path}.target"),
                type=self._str(c, f"{path}.type"),
                rules=tuple(self._list(c, f"{path}.rules", required=False)),
            )
            if column.type and column.type not in PARSERS:
                self.problems.append(
                    f"{path}.type: unknown type {column.type!r} (known: {', '.join(PARSERS)})"
                )
            for rule in column.rules:
                if rule not in RULES:
                    self.problems.append(
                        f"{path}.rules: unknown rule {rule!r} (known: {', '.join(RULES)})"
                    )
            columns.append(column)

        targets = [c.target for c in columns]
        for name in ("source", "target"):
            for value, n in Counter(getattr(c, name) for c in columns).items():
                if n > 1:
                    self.problems.append(f"dataset.columns: {name} {value!r} appears {n} times")

        unique_key = tuple(self._list(s, "dataset.unique_key"))
        for key in unique_key:
            if key not in targets:
                self.problems.append(f"dataset.unique_key: {key!r} is not a column target")

        on_conflict = self._str(s, "dataset.on_conflict", DatasetConfig.on_conflict)
        if on_conflict != "first_wins":
            self.problems.append("dataset.on_conflict: only 'first_wins' is supported")

        checks = tuple(self._list(s, "dataset.checks", required=False))
        for name in checks:
            if name not in CHECKS:
                self.problems.append(
                    f"dataset.checks: unknown check {name!r} (known: {', '.join(CHECKS)})"
                )
        return DatasetConfig(tuple(columns), unique_key, on_conflict, checks)

    # --- helpers ---------------------------------------------------------------

    def _section(self, raw: dict, name: str, build: Any, required: bool = True) -> Any:
        value = raw.get(name)
        if value is None and not required:
            return build(self, {})
        if not isinstance(value, dict):
            self.problems.append(
                f"{name}: section is missing" if value is None else f"{name}: must be a mapping"
            )
            # Build a placeholder without reporting each of the section's keys again.
            return build(_Builder([]), {})
        return build(self, value)

    def _known_keys(self, section: dict, path: str, cls: type) -> None:
        allowed = {f.name for f in fields(cls)}
        for key in section.keys() - allowed:
            where = f"{path}.{key}" if path else key
            self.problems.append(f"{where}: unknown setting (expected one of: "
                                 f"{', '.join(sorted(allowed))})")

    def _get(self, section: dict, path: str, default: Any) -> Any:
        key = path.rsplit(".", 1)[-1]
        value = section.get(key)
        if value is None or value == "":
            # An unset ${VAR} has already been reported for this path.
            already = any(p.startswith(f"{path}: ") for p in self.problems)
            if default is _REQUIRED and not already:
                self.problems.append(f"{path}: is required")
            return default
        return value

    def _str(self, section: dict, path: str, default: Any = _REQUIRED) -> Any:
        value = self._get(section, path, default)
        if value is _REQUIRED:
            return ""
        if value is not None and not isinstance(value, str):
            self.problems.append(f"{path}: must be a string")
            return str(value)
        return value

    def _positive_int(self, section: dict, path: str, default: int) -> int:
        value = self._get(section, path, default)
        # Strings are accepted because ${VAR} expansion always produces one.
        if isinstance(value, str) and value.strip().isdigit():
            value = int(value)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            self.problems.append(f"{path}: must be a positive integer")
            return default
        return value

    def _list(self, section: dict, path: str, required: bool = True) -> list[str]:
        value = self._get(section, path, _REQUIRED if required else [])
        if value is _REQUIRED:
            return []
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            self.problems.append(f"{path}: must be a list of strings")
            return []
        return value
