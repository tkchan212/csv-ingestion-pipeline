"""Stream rows out of a CSV file without loading it into memory."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


class HeaderError(ValueError):
    """The file has no header row, or the header does not match the dataset."""


@dataclass(frozen=True)
class RawRow:
    line_no: int              # 1-based line number where the row starts; header is line 1
    fields: list[str]
    raw_line: str             # original text, kept for rejected_record


class _LineRecorder:
    """Feeds lines to csv.reader and remembers the ones consumed since the last take().

    csv.reader pulls lines lazily, one record at a time, so after each record the
    buffer holds exactly that record's source text (several lines if a quoted
    field contains a newline).
    """

    def __init__(self, f: TextIO) -> None:
        self._f = f
        self._buf: list[str] = []

    def __iter__(self) -> _LineRecorder:
        return self

    def __next__(self) -> str:
        line = next(self._f)
        self._buf.append(line)
        return line

    def take(self) -> str:
        text = "".join(self._buf)
        self._buf.clear()
        return text


class CsvSource:
    def __init__(self, header: list[str], reader: Iterator[list[str]], lines: _LineRecorder):
        self.header = header
        self._reader = reader
        self._lines = lines

    def rows(self) -> Iterator[RawRow]:
        """Yield data rows one at a time. Blank lines are skipped."""
        next_line_no = self._reader.line_num + 1
        for fields in self._reader:
            raw = self._lines.take()
            line_no, next_line_no = next_line_no, self._reader.line_num + 1
            if not fields:
                continue
            yield RawRow(line_no, fields, raw.rstrip("\r\n"))


@contextmanager
def open_csv(path: Path, encoding: str = "utf-8", delimiter: str = ",") -> Iterator[CsvSource]:
    """Open `path`, read the header row and hand back a source to stream rows from."""
    with path.open(encoding=encoding, newline="") as f:
        lines = _LineRecorder(f)
        reader = csv.reader(lines, delimiter=delimiter)
        try:
            header = next(reader)
        except StopIteration:
            raise HeaderError("file is empty") from None
        lines.take()
        if header:
            header[0] = header[0].removeprefix("﻿")
        yield CsvSource([name.strip() for name in header], reader, lines)
