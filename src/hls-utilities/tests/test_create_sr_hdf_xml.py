from pathlib import Path
from typing import Literal

import pytest
from lxml import etree

from hls_utilities.create_landsat_sr_hdf_xml import create_landsat_sr_hdf_xml
from hls_utilities.create_sr_hdf_xml import create_sr_hdf_xml

TEST_DATA = Path(__file__).parent / "data"
INPUT_XML = TEST_DATA / "S2A_MSI_L1C_T17RKP_20200426_20200426.xml"


@pytest.fixture(autouse=True)
def espa_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ESPA_SCHEMA", str(TEST_DATA / "espa_internal_metadata_v2_2.xsd")
    )
    # espa writes temporary files relative to the current directory
    monkeypatch.chdir(tmp_path)


def _band_names(xml: Path) -> list[str]:
    root = etree.parse(xml).getroot()
    return [str(band.attrib["name"]) for band in root[1]]


@pytest.mark.parametrize(
    ("part", "expected"),
    [
        (
            "one",
            [
                "band01",
                "blue",
                "green",
                "red",
                "band05",
                "band06",
                "band07",
                "band08",
            ],
        ),
        ("two", ["band8a", "band09", "band10", "band11", "band12", "CLOUD"]),
    ],
)
def test_create_sr_hdf_xml(part: Literal["one", "two"], expected: list[str]) -> None:
    output_xml = Path(f"output_{part}.xml")
    create_sr_hdf_xml(INPUT_XML, output_xml, part)
    assert _band_names(output_xml) == expected


def test_create_landsat_sr_hdf_xml() -> None:
    # The Sentinel-2 fixture shares the sr_band1-7 and sr_aerosol_qa band names
    output_xml = Path("output.xml")
    create_landsat_sr_hdf_xml(INPUT_XML, output_xml)
    assert _band_names(output_xml) == [
        "band01",
        "band02-blue",
        "band03-green",
        "band04-red",
        "band05",
        "band06",
        "band07",
        "CLOUD",
    ]
