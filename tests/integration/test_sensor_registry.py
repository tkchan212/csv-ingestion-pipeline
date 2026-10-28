from pathlib import Path

import pytest

from ingestor.pipeline import process_file
from ingestor.registry import load_registry
from tests.conftest import make_config

pytestmark = pytest.mark.integration

REFERENCE_DIR = Path(__file__).parents[2] / "config" / "reference"
HEADER = "timestamp,sensorName,value"


@pytest.fixture
def config(tmp_path, database_url):
    return make_config(tmp_path, database_url)


@pytest.fixture
def registry(tmp_path):
    """A small reference directory; tests may overwrite its files."""
    directory = tmp_path / "reference"
    directory.mkdir()
    (directory / "sensor_types.csv").write_text(
        "code,name,unit,valid_min,valid_max\nTMP,Air temperature,°C,-20,60\n"
    )
    (directory / "buildings.csv").write_text("code,name\nHQT,Harbour Quay Tower\n")
    (directory / "sensors.csv").write_text(
        "name,type_code,building_code,floor\nHQT-TMP-03-01,TMP,HQT,3\nHQT-TMP-XX-01,TMP,HQT,\n"
    )
    return directory


def sensors(conn):
    return conn.execute(
        "SELECT name, type_code, building_code, floor FROM sensor ORDER BY name"
    ).fetchall()


def test_shipped_reference_data_loads(conn) -> None:
    counts = load_registry(conn, REFERENCE_DIR)

    assert counts["sensor_type"] == 4 and counts["building"] == 3 and counts["sensor"] > 0
    assert conn.execute(
        "SELECT count(*) FROM sensor_type WHERE valid_min IS NULL OR valid_max IS NULL"
    ).fetchone() == (0,)


def test_empty_cells_become_null(conn, registry) -> None:
    load_registry(conn, registry)

    assert sensors(conn) == [
        ("HQT-TMP-03-01", "TMP", "HQT", 3),
        ("HQT-TMP-XX-01", "TMP", "HQT", None),
    ]


def test_reloading_updates_metadata(conn, registry) -> None:
    load_registry(conn, registry)
    (registry / "sensors.csv").write_text(
        "name,type_code,building_code,floor\nHQT-TMP-03-01,TMP,HQT,4\n"
    )

    load_registry(conn, registry)

    assert sensors(conn)[0] == ("HQT-TMP-03-01", "TMP", "HQT", 4)


def test_sensor_with_an_unknown_building_is_refused(conn, registry) -> None:
    (registry / "sensors.csv").write_text(
        "name,type_code,building_code,floor\nZZZ-TMP-01-01,TMP,ZZZ,1\n"
    )

    with pytest.raises(Exception, match="building"):
        load_registry(conn, registry)
    assert sensors(conn) == []   # the whole load rolled back


def test_readings_join_to_their_meaning(conn, registry, config, write_csv) -> None:
    load_registry(conn, registry)
    path = write_csv("f1.csv", [HEADER, "2024-03-01T10:00:00Z,HQT-TMP-03-01,21.5"])

    process_file(conn, path, config)

    assert conn.execute(
        "SELECT sensor_name, value, unit, sensor_type, building, floor FROM sensor_reading_detail"
    ).fetchall() == [("HQT-TMP-03-01", 21.5, "°C", "Air temperature", "Harbour Quay Tower", 3)]
