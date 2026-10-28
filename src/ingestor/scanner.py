"""Find the files to ingest."""

from __future__ import annotations

from pathlib import Path


def discover_files(directory: Path, pattern: str) -> list[Path]:
    """Return matching regular files in `directory` (not recursive), sorted by name."""
    return sorted(p for p in directory.glob(pattern) if p.is_file())
