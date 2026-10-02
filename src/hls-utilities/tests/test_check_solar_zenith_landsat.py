import re
from pathlib import Path

import pytest

from hls_utilities.check_solar_zenith_landsat import solar_zenith_is_valid

TEST_DATA = Path(__file__).parent / "data"
MTL_C1 = TEST_DATA / "LC08_L1TP_027039_20190901_20190901_01_RT_MTL.txt"
MTL_C2 = TEST_DATA / "LC08_L1TP_231089_20200807_20200808_02_RT_MTL.txt"


@pytest.mark.parametrize("mtl", [MTL_C1, MTL_C2], ids=["c1", "c2"])
def test_solar_zenith_is_valid(mtl: Path):
    assert solar_zenith_is_valid(mtl)


@pytest.mark.parametrize("mtl", [MTL_C1, MTL_C2], ids=["c1", "c2"])
def test_solar_zenith_is_invalid(mtl: Path, tmp_path: Path):
    text = re.sub(r"SUN_ELEVATION = [\d.]+", "SUN_ELEVATION = 13.9", mtl.read_text())
    invalid_mtl = tmp_path / mtl.name
    invalid_mtl.write_text(text)
    assert not solar_zenith_is_valid(invalid_mtl)
