from pathlib import Path

import pytest

from ingestor.reader import HeaderError, open_csv


def read_all(path: Path):
    with open_csv(path) as source:
        return source.header, list(source.rows())


def test_reads_header_and_rows_with_line_numbers(write_csv) -> None:
    path = write_csv("a.csv", ["timestamp,sensorName,value", "t1,s1,1", "t2,s2,2"])

    header, rows = read_all(path)

    assert header == ["timestamp", "sensorName", "value"]
    assert [(r.line_no, r.fields, r.raw_line) for r in rows] == [
        (2, ["t1", "s1", "1"], "t1,s1,1"),
        (3, ["t2", "s2", "2"], "t2,s2,2"),
    ]


def test_blank_lines_are_skipped_but_still_counted(write_csv) -> None:
    path = write_csv("a.csv", ["h1,h2", "", "a,b", "", "c,d"])

    _, rows = read_all(path)

    assert [r.line_no for r in rows] == [3, 5]


def test_quoted_field_with_newline_keeps_its_start_line_and_raw_text(write_csv) -> None:
    path = write_csv("a.csv", ["h1,h2", 'x,"multi', 'line"', "y,z"])

    _, rows = read_all(path)

    assert rows[0].line_no == 2
    assert rows[0].fields == ["x", "multi\nline"]
    assert rows[0].raw_line == 'x,"multi\nline"'
    assert rows[1].line_no == 4


def test_strips_utf8_bom_and_whitespace_from_header(tmp_path: Path) -> None:
    path = tmp_path / "bom.csv"
    path.write_text("﻿timestamp , sensorName,value\n", encoding="utf-8")

    header, _ = read_all(path)

    assert header == ["timestamp", "sensorName", "value"]


def test_empty_file_raises_header_error(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("")

    with pytest.raises(HeaderError):
        read_all(path)
