from pathlib import Path

import pytest

from ingestor.cli import main

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("test_sensors")]

SHIPPED = Path(__file__).parents[2] / "config" / "pipeline.yaml"
HEADER = "timestamp,sensorName,value"


@pytest.fixture
def cli(tmp_path, database_url):
    def run(*extra: str) -> int:
        return main(["--config", str(SHIPPED), "--input-dir", str(tmp_path),
                     "--db-url", database_url, *extra])
    return run


def test_loads_the_directory_and_exits_0(conn, cli, write_csv) -> None:
    write_csv("a.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    write_csv("b.csv", [HEADER, "2024-03-01T10:00:00Z,s2,2", "bad,s2,3"])

    assert cli() == 0
    assert conn.execute("SELECT count(*) FROM sensor_reading").fetchone() == (2,)
    assert cli() == 0   # rerun: both skipped


def test_exits_1_when_a_file_fails(conn, cli, write_csv) -> None:
    write_csv("a.csv", [HEADER, "2024-03-01T10:00:00Z,s1,1"])
    write_csv("b.csv", ["wrong,header"])

    assert cli() == 1
    assert conn.execute("SELECT count(*) FROM sensor_reading").fetchone() == (1,)


def test_exits_2_for_a_missing_directory(cli, tmp_path) -> None:
    assert main(["--config", str(SHIPPED), "--input-dir", str(tmp_path / "nope"),
                 "--db-url", "postgresql://unused/x"]) == 2


def test_exits_2_for_an_invalid_config(tmp_path, caplog) -> None:
    config = tmp_path / "bad.yaml"
    config.write_text("source: {}\n")

    assert main(["--config", str(config)]) == 2
    assert "source.directory: is required" in caplog.text


def test_exits_2_when_the_database_is_unreachable(tmp_path) -> None:
    assert main(["--config", str(SHIPPED), "--input-dir", str(tmp_path),
                 "--db-url", "postgresql://nobody@127.0.0.1:1/x?connect_timeout=1"]) == 2
