import re
from pathlib import Path

from hls_utilities.check_solar_zenith_sentinel import solar_zenith_is_valid

TEST_DATA = Path(__file__).parent / "data"


def test_solar_zenith_is_valid():
    assert solar_zenith_is_valid(TEST_DATA / "MTD_TL.xml")


def test_solar_zenith_is_invalid(tmp_path: Path):
    xml = (TEST_DATA / "MTD_TL.xml").read_text()
    # Only the first ZENITH_ANGLE is the mean sun angle
    xml = re.sub(
        r'<ZENITH_ANGLE unit="deg">[\d.]+<',
        '<ZENITH_ANGLE unit="deg">76.1<',
        xml,
        count=1,
    )
    mtd = tmp_path / "MTD_TL.xml"
    mtd.write_text(xml)
    assert not solar_zenith_is_valid(mtd)
