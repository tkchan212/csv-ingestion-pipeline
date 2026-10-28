from pathlib import Path

from ingestor.scanner import discover_files


def test_discover_files_is_flat_sorted_and_filtered(tmp_path: Path) -> None:
    (tmp_path / "b.csv").write_text("x")
    (tmp_path / "a.csv").write_text("x")
    (tmp_path / "notes.txt").write_text("x")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "c.csv").write_text("x")

    assert [p.name for p in discover_files(tmp_path, "*.csv")] == ["a.csv", "b.csv"]
