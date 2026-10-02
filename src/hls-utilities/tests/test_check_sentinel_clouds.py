import re
from pathlib import Path

from hls_utilities.check_sentinel_clouds import cloud_cover_is_valid

TEST_DATA = Path(__file__).parent / "data"


def test_cloud_cover_is_valid():
    assert cloud_cover_is_valid(TEST_DATA / "MTD_MSIL1C.xml")


def test_cloud_cover_is_invalid(tmp_path: Path):
    xml = (TEST_DATA / "MTD_MSIL1C.xml").read_text()
    xml = re.sub(
        r"<Cloud_Coverage_Assessment>[\d.]+<",
        "<Cloud_Coverage_Assessment>95.1<",
        xml,
    )
    mtd = tmp_path / "MTD_MSIL1C.xml"
    mtd.write_text(xml)
    assert not cloud_cover_is_valid(mtd)
