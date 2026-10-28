"""Turn raw CSV fields into typed values, or a rejection reason.

Parsers ("type" in the config) and extra rules are looked up by name in the
registries below, so a new dataset only needs YAML unless it needs a new type.

Rejection reasons deliberately leave out the offending value (raw_line already
has it), so they stay stable and can be grouped on: `GROUP BY reason`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ingestor.config import ColumnConfig
from ingestor.reader import HeaderError, RawRow

Parser = Callable[[str], Any]           # raises ValueError with a short reason
Rule = Callable[[Any], str | None]      # returns a reason, or None if the value passes

PARSERS: dict[str, Parser] = {}
RULES: dict[str, Rule] = {}


def parser(name: str) -> Callable[[Parser], Parser]:
    def register(fn: Parser) -> Parser:
        PARSERS[name] = fn
        return fn
    return register


def rule(name: str) -> Callable[[Rule], Rule]:
    def register(fn: Rule) -> Rule:
        RULES[name] = fn
        return fn
    return register


@parser("string")
def parse_string(text: str) -> str:
    if "\x00" in text:
        raise ValueError("contains a NUL character")
    return text


# Calendar date, "T", then at least hours and minutes. fromisoformat alone would
# also accept date-only values and a space separator.
_ISO_DATETIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")


@parser("timestamp_tz")
def parse_timestamp_tz(text: str) -> datetime:
    if not _ISO_DATETIME.match(text):
        raise ValueError("not an ISO 8601 date-time")
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError("not an ISO 8601 date-time") from None
    if value.tzinfo is None:
        raise ValueError("timestamp has no time zone")
    return value.astimezone(UTC)


# Plain decimal or scientific notation. float() alone would also accept
# "nan", "inf" and "1_000".
_DECIMAL = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")


@parser("float")
def parse_float(text: str) -> float:
    if not _DECIMAL.fullmatch(text):
        raise ValueError("not a number")
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("number out of range")
    return value


@rule("non_empty")
def non_empty(value: Any) -> str | None:
    return "must not be empty" if value == "" else None


@dataclass(frozen=True)
class ValidRecord:
    line_no: int
    values: dict[str, Any]    # keyed by target column name


@dataclass(frozen=True)
class Rejection:
    line_no: int
    raw_line: str
    reason: str


def _lookup(registry: dict[str, Any], kind: str, name: str) -> Any:
    try:
        return registry[name]
    except KeyError:
        known = ", ".join(sorted(registry))
        raise ValueError(f"unknown {kind} {name!r} (known: {known})") from None


class RecordValidator:
    """Validates rows of one file, given the file's header."""

    def __init__(self, columns: Sequence[ColumnConfig], header: Sequence[str]) -> None:
        missing = [c.source for c in columns if c.source not in header]
        if missing:
            raise HeaderError(f"header is missing column(s): {', '.join(missing)}")
        self._width = len(header)
        self._plan = [
            (
                header.index(c.source),
                c,
                _lookup(PARSERS, "type", c.type),
                [_lookup(RULES, "rule", r) for r in c.rules],
            )
            for c in columns
        ]

    def validate(self, row: RawRow) -> ValidRecord | Rejection:
        if len(row.fields) != self._width:
            return Rejection(
                row.line_no, row.raw_line, f"expected {self._width} fields, got {len(row.fields)}"
            )
        values: dict[str, Any] = {}
        for index, column, parse, rules in self._plan:
            try:
                value = parse(row.fields[index].strip())
            except ValueError as exc:
                return Rejection(row.line_no, row.raw_line, f"{column.source}: {exc}")
            for check in rules:
                if (problem := check(value)) is not None:
                    return Rejection(row.line_no, row.raw_line, f"{column.source}: {problem}")
            values[column.target] = value
        return ValidRecord(row.line_no, values)
