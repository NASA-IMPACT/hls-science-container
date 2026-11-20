from collections.abc import Sequence
from pathlib import Path

import pytest
from lxml import etree

from hls_utilities import lasrc_encoding as enc
from hls_utilities.register_lasrc_bands import (
    LANDSAT_SR_BANDS,
    LANDSAT_TOA_BT_BANDS,
    SENTINEL_SR_BANDS,
    read_product_id,
    register_lasrc_bands,
)

TEST_DATA = Path(__file__).parent / "data"
LASRC_DATA = TEST_DATA / "lasrc"
NS = {"e": "http://espa.cr.usgs.gov/v2"}
LASRC_PRODUCTS = {"sr_refl", "toa_refl", "toa_bt"}
PRODUCTION_DATE = "{http://espa.cr.usgs.gov/v2}production_date"
AERO_BANDS = ("sr_aerosol", "sr_aerosol_qa")
S2_XML = LASRC_DATA / "sentinel_espa_pre_lasrc.xml"


@pytest.fixture(autouse=True)
def espa_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ESPA_SCHEMA", str(TEST_DATA / "espa_internal_metadata_v2_2.xsd")
    )


def _touch_outputs(xml: Path, names: Sequence[str]) -> None:
    product_id = read_product_id(xml)
    for name in (*names, *AERO_BANDS):
        (xml.parent / f"{product_id}_{name}.img").touch()


def _tree(el: etree._Element) -> tuple:
    return (
        el.tag,
        sorted(el.attrib.items()),
        (el.text or "").strip(),
        [_tree(c) for c in el if c.tag != PRODUCTION_DATE],
    )


def _bands(xml: Path) -> list[etree._Element]:
    root = etree.parse(str(xml)).getroot()
    return list(root.find("e:bands", NS))


def _strip_lasrc_bands(src: Path, dst: Path) -> list[etree._Element]:
    tree = etree.parse(str(src))
    bands = tree.getroot().find("e:bands", NS)
    removed = [b for b in bands if b.get("product") in LASRC_PRODUCTS]
    for band in removed:
        bands.remove(band)
    tree.write(str(dst), xml_declaration=True, encoding="UTF-8")
    return removed


_S2_C_XML = LASRC_DATA / "sentinel_c_lasrc.xml"


@pytest.mark.parametrize(
    "fixture",
    [
        "landsat_c_lasrc.xml",
        pytest.param(
            "sentinel_c_lasrc.xml",
            marks=pytest.mark.skipif(
                not _S2_C_XML.exists(), reason="no C Sentinel-2 reference XML"
            ),
        ),
    ],
)
def test_registration_matches_c(fixture: str, tmp_path: Path) -> None:
    xml = tmp_path / "scene.xml"
    removed = _strip_lasrc_bands(LASRC_DATA / fixture, xml)
    names = [b.get("name") for b in removed if b.get("name") not in AERO_BANDS]
    sr = next(b for b in removed if b.get("source"))
    app_version = removed[0].findtext("e:app_version", namespaces=NS)
    _touch_outputs(xml, [str(n) for n in names])

    register_lasrc_bands(
        xml,
        [str(n) for n in names],
        app_version=str(app_version),
        aux_source=str(sr.get("source")),
    )

    actual = _bands(xml)[-len(removed) :]
    assert [_tree(b) for b in actual] == [_tree(b) for b in removed]


# Real S2 ESPA XML from a C `sentinel` run with the working dir kept, taken just
# before LaSRC (<SAFE>/<product_id>.xml after PrepareEspaInput).
def test_sentinel_registration_structure(tmp_path: Path) -> None:
    xml = tmp_path / "scene.xml"
    xml.write_bytes(S2_XML.read_bytes())
    n_before = len(_bands(xml))
    _touch_outputs(xml, SENTINEL_SR_BANDS)
    aux = "VJ104ANC.A2026024.002.2026025085111.h5"

    register_lasrc_bands(
        xml,
        SENTINEL_SR_BANDS,
        app_version="LaSRC_3.5.1.0 (Collection 2)",
        aux_source=aux,
    )

    bands = _bands(xml)
    new = bands[n_before:]
    assert [b.get("name") for b in new] == list(SENTINEL_SR_BANDS + AERO_BANDS)

    by_name = {b.get("name"): b for b in bands}
    b02 = by_name["B02"]
    b01 = by_name["B01"]
    product_id = (
        etree.parse(str(xml))
        .getroot()
        .findtext("e:global_metadata/e:product_id", namespaces=NS)
    )
    for band in new:
        name = str(band.get("name"))
        assert band.get("nlines") == b02.get("nlines") == "10980"
        assert band.get("nsamps") == b02.get("nsamps")
        assert (
            band.find("e:pixel_size", NS).attrib == b02.find("e:pixel_size", NS).attrib
        )
        prefix = str(b01.findtext("e:short_name", namespaces=NS))[:7]
        suffix = {"sr_aerosol": "AERO", "sr_aerosol_qa": "AEROQA"}.get(name, "SR")
        assert band.findtext("e:short_name", namespaces=NS) == prefix + suffix
        assert band.findtext("e:file_name", namespaces=NS) == f"{product_id}_{name}.img"
        if name == "sr_aerosol":
            assert float(band.get("scale_factor")) == enc.SCALE_FACTOR_AERO
            assert int(band.get("fill_value")) == enc.FILL_VALUE_AERO
        elif name == "sr_aerosol_qa":
            assert band.get("fill_value") == "1"
        else:
            assert float(band.get("scale_factor")) == enc.SCALE_FACTOR_REFL
            assert float(band.get("add_offset")) == enc.OFFSET_REFL
            assert int(band.get("fill_value")) == enc.FILL_VALUE
            assert band.get("source") == aux
            assert band.findtext("e:level1_filename", namespaces=NS) == b01.findtext(
                "e:level1_filename", namespaces=NS
            )


def test_band_name_constants() -> None:
    assert LANDSAT_SR_BANDS == tuple(f"sr_band{i}" for i in range(1, 8))
    assert LANDSAT_TOA_BT_BANDS == ("toa_band9", "bt_band10", "bt_band11")
    assert SENTINEL_SR_BANDS[8] == "sr_band8a"
    assert len(SENTINEL_SR_BANDS) == 13


def test_registration_rejects_existing_bands(tmp_path: Path) -> None:
    xml = tmp_path / "scene.xml"
    _strip_lasrc_bands(LASRC_DATA / "landsat_c_lasrc.xml", xml)
    _touch_outputs(xml, LANDSAT_SR_BANDS)
    kwargs = {"app_version": "LaSRC_3.5.1.0 (Collection 2)", "aux_source": "aux.h5"}
    register_lasrc_bands(xml, LANDSAT_SR_BANDS, **kwargs)
    n_bands = len(_bands(xml))

    with pytest.raises(ValueError, match="sr_band1"):
        register_lasrc_bands(xml, LANDSAT_SR_BANDS, **kwargs)
    assert len(_bands(xml)) == n_bands


def test_registration_requires_band_files(tmp_path: Path) -> None:
    xml = tmp_path / "scene.xml"
    _strip_lasrc_bands(LASRC_DATA / "landsat_c_lasrc.xml", xml)
    _touch_outputs(xml, LANDSAT_SR_BANDS)
    product_id = read_product_id(xml)
    (tmp_path / f"{product_id}_sr_band3.img").unlink()
    (tmp_path / f"{product_id}_sr_aerosol_qa.img").unlink()
    before = xml.read_bytes()

    with pytest.raises(FileNotFoundError, match="sr_band3.*sr_aerosol_qa"):
        register_lasrc_bands(
            xml,
            LANDSAT_SR_BANDS,
            app_version="LaSRC_3.5.1.0 (Collection 2)",
            aux_source="aux.h5",
        )
    assert xml.read_bytes() == before
