from datetime import UTC, datetime

import pytest

from ingestor.config import ColumnConfig
from ingestor.reader import HeaderError, RawRow
from ingestor.validation import (
    RecordValidator,
    Rejection,
    ValidRecord,
    parse_float,
    parse_timestamp_tz,
)
from tests.conftest import SENSOR_COLUMNS

HEADER = ["timestamp", "sensorName", "value"]


def validate(*fields: str):
    row = RawRow(line_no=2, fields=list(fields), raw_line=",".join(fields))
    return RecordValidator(SENSOR_COLUMNS, HEADER).validate(row)


# --- timestamps ---------------------------------------------------------------

@pytest.mark.parametrize(
    "text, expected",
    [
        ("2024-03-01T10:15:30Z", datetime(2024, 3, 1, 10, 15, 30, tzinfo=UTC)),
        ("2024-03-01T18:15:30+08:00", datetime(2024, 3, 1, 10, 15, 30, tzinfo=UTC)),
        ("2024-03-01T10:15:30.123456-05:00",
         datetime(2024, 3, 1, 15, 15, 30, 123456, tzinfo=UTC)),
        ("2024-03-01T10:15+00:00", datetime(2024, 3, 1, 10, 15, tzinfo=UTC)),
    ],
)
def test_timestamp_with_zone_is_normalised_to_utc(text: str, expected: datetime) -> None:
    assert parse_timestamp_tz(text) == expected
    assert parse_timestamp_tz(text).utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    "text, reason",
    [
        ("2024-03-01T10:15:30", "timestamp has no time zone"),
        ("2024-03-01", "not an ISO 8601 date-time"),
        ("2024-03-01 10:15:30Z", "not an ISO 8601 date-time"),
        ("01/03/2024 10:15", "not an ISO 8601 date-time"),
        ("2024-13-01T10:15:30Z", "not an ISO 8601 date-time"),
        ("yesterday", "not an ISO 8601 date-time"),
        ("", "not an ISO 8601 date-time"),
    ],
)
def test_invalid_timestamps_are_rejected(text: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        parse_timestamp_tz(text)


# --- numeric values -----------------------------------------------------------

@pytest.mark.parametrize(
    "text, expected",
    [("42", 42.0), ("-3.5", -3.5), ("+0.25", 0.25), (".5", 0.5), ("5.", 5.0), ("1.2e3", 1200.0)],
)
def test_numeric_values_are_parsed(text: str, expected: float) -> None:
    assert parse_float(text) == expected


@pytest.mark.parametrize("text", ["true", "ABC", "", "NaN", "inf", "-Infinity", "1_000", "1,5"])
def test_non_numeric_values_are_rejected(text: str) -> None:
    with pytest.raises(ValueError, match="not a number"):
        parse_float(text)


def test_overflowing_value_is_rejected() -> None:
    with pytest.raises(ValueError, match="out of range"):
        parse_float("1e400")


# --- whole rows ---------------------------------------------------------------

def test_valid_row_maps_to_target_columns_with_whitespace_trimmed() -> None:
    result = validate("2024-03-01T10:15:30Z", "  temp-1 ", " 21.5")

    assert result == ValidRecord(
        line_no=2,
        values={
            "ts": datetime(2024, 3, 1, 10, 15, 30, tzinfo=UTC),
            "sensor_name": "temp-1",
            "value": 21.5,
        },
    )


@pytest.mark.parametrize(
    "fields, reason",
    [
        (("2024-03-01T10:15:30Z", "s1"), "expected 3 fields, got 2"),
        (("2024-03-01T10:15:30Z", "s1", "1", "extra"), "expected 3 fields, got 4"),
        (("not-a-date", "s1", "1"), "timestamp: not an ISO 8601 date-time"),
        (("2024-03-01T10:15:30Z", "   ", "1"), "sensorName: must not be empty"),
        (("2024-03-01T10:15:30Z", "s1", "true"), "value: not a number"),
    ],
)
def test_invalid_rows_are_rejected_with_a_reason(fields: tuple[str, ...], reason: str) -> None:
    result = validate(*fields)

    assert isinstance(result, Rejection)
    assert result.reason == reason
    assert result.raw_line == ",".join(fields)


def test_columns_are_found_by_header_name_not_position() -> None:
    validator = RecordValidator(SENSOR_COLUMNS, ["value", "timestamp", "sensorName"])

    result = validator.validate(RawRow(2, ["7", "2024-03-01T10:15:30Z", "s1"], ""))

    assert isinstance(result, ValidRecord)
    assert result.values["value"] == 7.0


def test_header_missing_a_column_raises() -> None:
    with pytest.raises(HeaderError, match="sensorName"):
        RecordValidator(SENSOR_COLUMNS, ["timestamp", "value"])


def test_unknown_type_in_config_raises() -> None:
    columns = (ColumnConfig(source="a", target="a", type="uuid"),)

    with pytest.raises(ValueError, match="unknown type 'uuid'"):
        RecordValidator(columns, ["a"])
