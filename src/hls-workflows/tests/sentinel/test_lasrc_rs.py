from __future__ import annotations

import shutil
from pathlib import Path
from typing import cast

import pytest
from lxml import etree

from hls_utilities.register_lasrc_bands import SENTINEL_SR_BANDS
from hls_workflows.sentinel import lasrc_rs
from hls_workflows.sentinel.assets import (
    CONFIG,
    EnvConfig,
    espa_xml_asset,
    lasrc_aerosol_qa_asset,
    rust_lasrc_dir_asset,
)

UTIL_TESTS = Path(__file__).parents[3] / "hls-utilities" / "tests" / "data"
# Real S2 ESPA XML from a C `sentinel` run with the working dir kept, taken just
# before LaSRC (<SAFE>/<product_id>.xml after PrepareEspaInput).
FIXTURE = UTIL_TESTS / "lasrc" / "sentinel_espa_pre_lasrc.xml"
SCHEMA = UTIL_TESTS / "espa_internal_metadata_v2_2.xsd"
GRANULE_ID = "S2B_MSIL1C_20260124T092241_N0511_R093_T38PNC_20260124T112233"
PRODUCT_ID = "S2B_MSI_L1C_T38PNC_20260124_20260124"
AUX_NAME = "VJ104ANC.A2026024.002.2026025112957.h5"
RUST_OUTPUTS = (*SENTINEL_SR_BANDS, "sr_aerosol", "sr_aerosol_qa")


def test_register_s2_renames_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ESPA_SCHEMA", str(SCHEMA))
    monkeypatch.setattr(
        lasrc_rs,
        "resolve_lasrc_aux_paths",
        lambda **_: {"wv_oz_hdf": tmp_path / AUX_NAME},
    )
    monkeypatch.setattr(lasrc_rs, "espa_app_version", lambda: "LaSRC_test")
    safe_dir = tmp_path / GRANULE_ID / f"{GRANULE_ID}.SAFE"
    safe_dir.mkdir(parents=True)
    # The ESPA XML stem need not equal global_metadata/product_id
    xml = safe_dir / "espa_scene.xml"
    shutil.copy(FIXTURE, xml)

    rust_dir = tmp_path / GRANULE_ID / "lasrc_rs"
    rust_dir.mkdir()
    for name in RUST_OUTPUTS:
        (rust_dir / f"{GRANULE_ID}_{name}.img").touch()
        (rust_dir / f"{GRANULE_ID}_{name}.hdr").touch()
    (rust_dir / f"{GRANULE_ID}_sr_band1.img.aux.xml").touch()

    config = EnvConfig(
        job_id="job",
        granule_ids=[GRANULE_ID],
        input_bucket="in",
        output_bucket="out",
        gibs_bucket="gibs",
        working_dir=tmp_path,
        ac_code="LaSRC",
        cloud_masking_code="Fmask",
    )
    result = lasrc_rs.RegisterLaSRC.map(GRANULE_ID)("RegisterLaSRC").run(
        {
            CONFIG: config,
            espa_xml_asset(GRANULE_ID): xml,
            rust_lasrc_dir_asset(GRANULE_ID): rust_dir,
        }
    )

    expected = safe_dir / f"{PRODUCT_ID}_sr_aerosol_qa.img"
    assert result == {lasrc_aerosol_qa_asset(GRANULE_ID): expected}
    assert list(rust_dir.iterdir()) == []

    ns = {"e": "http://espa.cr.usgs.gov/v2"}
    file_names = etree.parse(str(xml)).xpath(
        "//e:band/e:file_name/text()", namespaces=ns
    )
    registered = [str(f) for f in cast(list[str], file_names) if "_sr_" in str(f)]
    assert len(registered) == len(RUST_OUTPUTS)
    for name in registered:
        assert (safe_dir / name).exists()
    assert (safe_dir / f"{PRODUCT_ID}_sr_band1.hdr").exists()
    assert (safe_dir / f"{PRODUCT_ID}_sr_band1.img.aux.xml").exists()
