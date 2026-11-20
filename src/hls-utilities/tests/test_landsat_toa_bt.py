import shutil
from pathlib import Path

import numpy as np
import pytest
from lxml import etree

from hls_utilities.landsat_toa_bt import (
    brightness_temperature,
    level1_qa_is_fill,
    read_toa_bt_params,
    toa_reflectance,
)
from hls_utilities.lasrc_encoding import encode_reflectance, encode_thermal

DATA = Path(__file__).parent / "data" / "lasrc"
TEST_DATA = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def espa_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ESPA_SCHEMA", str(TEST_DATA / "espa_internal_metadata_v2_2.xsd")
    )
    monkeypatch.chdir(tmp_path)


def _oli_only_xml(tmp_path: Path) -> Path:
    out = tmp_path / "oli_only.xml"
    shutil.copy(DATA / "landsat_c_lasrc.xml", out)
    tree = etree.parse(out)
    root = tree.getroot()
    for el in root.iter():
        if etree.QName(el).localname == "band" and el.get("name") in ("b10", "b11"):
            el.getparent().remove(el)
    for el in root.iter():
        if etree.QName(el).localname == "instrument":
            el.text = "OLI"
    tree.write(str(out))
    return out


def test_toa_band9_matches_c_bit_for_bit() -> None:
    w = np.load(DATA / "landsat_toa_bt_window.npz")
    p = read_toa_bt_params(DATA / "landsat_c_lasrc.xml")
    fill = level1_qa_is_fill(w["qa_pixel"])
    toa = toa_reflectance(
        w["b9"], w["sza"], fill, p.refl_gain_b9, p.refl_bias_b9, p.sza_gain, p.sza_bias
    )
    np.testing.assert_array_equal(encode_reflectance(toa), w["c_toa_band9"])


@pytest.mark.parametrize(("band", "i"), [("b10", 0), ("b11", 1)])
def test_bt_matches_c_bit_for_bit(band: str, i: int) -> None:
    w = np.load(DATA / "landsat_toa_bt_window.npz")
    p = read_toa_bt_params(DATA / "landsat_c_lasrc.xml")
    assert p.rad_gain and p.rad_bias and p.k1 and p.k2
    fill = level1_qa_is_fill(w["qa_pixel"])
    bt = brightness_temperature(
        w[band], fill, p.rad_gain[i], p.rad_bias[i], p.k1[i], p.k2[i]
    )
    np.testing.assert_array_equal(encode_thermal(bt), w[f"c_bt_band1{i}"])


def test_zero_toa_is_written_as_fill() -> None:
    out = encode_reflectance(np.array([0.0, -0.2, 0.5], np.float32))
    assert out.tolist() == [0, 0, 25455]


def test_saturated_values_truncate_like_c() -> None:
    assert encode_reflectance(np.array([np.float32(1.6)])).tolist() == [65454]
    assert encode_thermal(np.array([np.float32(350.0)])).tolist() == [58805]


def test_oli_only_skips_bt(tmp_path: Path) -> None:
    p = read_toa_bt_params(_oli_only_xml(tmp_path))
    assert p.rad_gain is None and p.k1 is None
