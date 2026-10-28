"""Checks against reference data, run in the database while a file is merged.

A row that parses (validation.py) can still make no sense: its sensor is not in
the registry, or its value is impossible for the sensor's type. Such rows are
moved from staging to quarantined_reading instead of being loaded.

Like parsers and rules, checks are named here and switched on in the dataset
config (`dataset.checks`). Each is a SQL condition on the staging row, aliased
`s`, that is true when the row FAILS. Unlike parsers they are specific to a
dataset's columns and reference tables, so a new dataset brings its own checks.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    reason: str
    fails_when: str           # SQL condition on staging alias `s`


CHECKS: dict[str, Check] = {
    "known_sensor": Check(
        reason="sensor is not in the registry",
        fails_when="NOT EXISTS (SELECT 1 FROM sensor x WHERE x.name = s.sensor_name)",
    ),
    "value_in_type_range": Check(
        reason="value is outside the valid range for the sensor type",
        fails_when="""EXISTS (
            SELECT 1 FROM sensor x JOIN sensor_type t ON t.code = x.type_code
            WHERE x.name = s.sensor_name
              AND (s.value < t.valid_min OR s.value > t.valid_max)
        )""",
    ),
}
