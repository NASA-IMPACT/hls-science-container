from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from lxml import etree

from hls_workflows.base import TaskFailure
from hls_workflows.landsat_ac import lasrc_rs
from hls_workflows.landsat_ac.assets import (
    CONFIG,
    ESPA_XML,
    LASRC_DONE,
    RUST_LASRC_DONE,
    SOLAR_VALID,
    EnvConfig,
)

UTIL_TESTS = Path(__file__).parents[3] / "hls-utilities" / "tests" / "data"
FIXTURE = UTIL_TESTS / "lasrc" / "landsat_c_lasrc.xml"
SCHEMA = UTIL_TESTS / "espa_internal_metadata_v2_2.xsd"
PRODUCT_ID = "LC08_L1TP_184033_20260103_20260107_02_T1"
AUX_NAME = "VJ104ANC.A2026003.002.2026004112957.h5"
STRIPPED = {"sr_refl", "toa_refl", "toa_bt"}
RUST_OUTPUTS = (
    *(f"sr_band{i}" for i in (1, 2, 3, 4, 5, 6, 7, 9)),
    "sr_aerosol",
    "sr_aerosol_qa",
)


def _band_names(xml: Path) -> list[str]:
    ns = {"e": "http://espa.cr.usgs.gov/v2"}
    return [
        str(n) for n in etree.parse(str(xml)).xpath("//e:band/@name", namespaces=ns)
    ]


@pytest.fixture
def granule(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, EnvConfig]:
    """A granule dir holding Rust LaSRC output and an ESPA XML without LaSRC bands."""
    monkeypatch.setenv("ESPA_SCHEMA", str(SCHEMA))
    tree = etree.parse(str(FIXTURE))
    for band in tree.xpath("//*[local-name()='band']"):
        if band.get("product") in STRIPPED:
            band.getparent().remove(band)
    xml = tmp_path / f"{PRODUCT_ID}.xml"
    tree.write(str(xml), xml_declaration=True, encoding="UTF-8")

    for name in RUST_OUTPUTS:
        (tmp_path / f"{PRODUCT_ID}_{name}.img").touch()
        (tmp_path / f"{PRODUCT_ID}_{name}.hdr").touch()
    (tmp_path / f"{PRODUCT_ID}_sr_band9.img.aux.xml").touch()

    def fake_toa_bt(espa_xml: Path) -> list[str]:
        names = ["toa_band9", "bt_band10", "bt_band11"]
        for n in names:
            (espa_xml.parent / f"{PRODUCT_ID}_{n}.img").touch()
        return names

    monkeypatch.setattr(lasrc_rs, "write_landsat_toa_bt", fake_toa_bt)
    monkeypatch.setattr(
        lasrc_rs,
        "resolve_lasrc_aux_paths",
        lambda **_: {"wv_oz_hdf": tmp_path / AUX_NAME},
    )
    config = EnvConfig(
        job_id="job",
        granule=PRODUCT_ID,
        input_bucket="in",
        output_bucket="out",
        ac_code="LaSRC",
        cloud_masking_code="Fmask",
        working_dir=tmp_path,
        granule_dir=tmp_path,
    )
    return xml, config


def test_register_landsat_removes_sr_band9(
    granule: tuple[Path, EnvConfig],
) -> None:
    xml, config = granule
    result = lasrc_rs.RegisterLaSRC("RegisterLaSRC").run(
        {CONFIG: config, ESPA_XML: xml, RUST_LASRC_DONE: True}
    )

    assert result == {LASRC_DONE: True}
    for suffix in (".img", ".hdr", ".img.aux.xml"):
        assert not (xml.parent / f"{PRODUCT_ID}_sr_band9{suffix}").exists()
    names = _band_names(xml)
    assert "sr_band9" not in names
    sr_start = names.index("sr_band1")
    assert names[sr_start - 3 : sr_start] == ["toa_band9", "bt_band10", "bt_band11"]
    assert names[sr_start:] == [
        *(f"sr_band{i}" for i in range(1, 8)),
        "sr_aerosol",
        "sr_aerosol_qa",
    ]


def test_register_fails_when_band_file_missing(
    granule: tuple[Path, EnvConfig],
) -> None:
    xml, config = granule
    (xml.parent / f"{PRODUCT_ID}_sr_band3.img").unlink()

    with pytest.raises(TaskFailure, match="sr_band3"):
        lasrc_rs.RegisterLaSRC("RegisterLaSRC").run(
            {CONFIG: config, ESPA_XML: xml, RUST_LASRC_DONE: True}
        )


def test_run_rejects_pre_viirs_acquisition(
    granule: tuple[Path, EnvConfig], monkeypatch: pytest.MonkeyPatch
) -> None:
    xml, config = granule
    old = replace(config, granule="LC08_L1TP_184033_20200103_20200107_02_T1")

    def fail(**_: object) -> None:
        raise AssertionError("aux lookup reached")

    monkeypatch.setattr(lasrc_rs, "resolve_lasrc_aux_paths", fail)
    monkeypatch.delenv("VIIRS_AUX_STARTING_DATE", raising=False)

    with pytest.raises(TaskFailure, match="pre-VIIRS"):
        lasrc_rs.RunLaSRCRust("LaSRC").run(
            {CONFIG: old, ESPA_XML: xml, SOLAR_VALID: True}
        )
