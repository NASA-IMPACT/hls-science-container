from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.base import TaskFailure
from hls_nextgen_orchestration.common import Paths
from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.sentinel.assets import (
    CONFIG,
    EnvConfig,
    angle_hdf_asset,
    combined_sr_hdf_asset,
    detfoo_file_asset,
    espa_xml_asset,
    final_sr_hdf_asset,
    fmask_bin_asset,
    granule_dir_asset,
    lasrc_aerosol_qa_asset,
    mtd_tl_asset,
    quality_mask_applied_asset,
    safe_dir_asset,
    solar_valid_asset,
    split_hdf_parts_asset,
    trimmed_hdf_asset,
)
from hls_nextgen_orchestration.sentinel.mapped_tasks import (
    AddS2FmaskSds,
    ApplyS2QualityMask,
    CheckSolarZenith,
    CombineS2Hdf,
    DeriveS2Angles,
    DownloadSentinelGranule,
    FindS2Footprint,
    GetGranuleDir,
    ProcessHdfParts,
    RunFmask,
    TrimS2Hdf,
)


def test_download_sentinel(
    s3_client: S3Client,
    sentinel_config: EnvConfig,
    tmp_path: Path,
    populate_sentinel_safe: Callable[[Path, Sentinel2Granule], Path],
) -> None:
    """Tests that the granule is downloaded and unzipped."""
    granule_id = "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
    granule = Sentinel2Granule.from_str(granule_id)
    safe_zip = populate_sentinel_safe(tmp_path, granule)

    s3_client.create_bucket(Bucket=sentinel_config.input_bucket)
    s3_client.upload_file(
        Filename=str(safe_zip),
        Bucket=sentinel_config.input_bucket,
        Key=f"{granule_id}.zip",
    )

    task = DownloadSentinelGranule.map(granule_id)(name="download")
    outputs = task.run({CONFIG: sentinel_config})

    safe_dir = safe_dir_asset(granule_id)
    assert safe_dir in outputs
    assert outputs[safe_dir].exists()


def test_get_granule_dir(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests finding the internal GRANULE directory."""
    # Setup structure
    granule_id = "GRANULE_ID"
    safe_dir = sentinel_config.working_dir / granule_id / "PRODUCT_ID.SAFE"
    inner = safe_dir / "GRANULE" / granule_id
    inner.mkdir(parents=True)
    xml = inner / "MTD_TL.xml"
    xml.touch()

    task = GetGranuleDir.map(granule_id)(name="get_dir")
    outputs = task.run({safe_dir_asset(granule_id): safe_dir})

    assert outputs[granule_dir_asset(granule_id)] == inner
    assert outputs[mtd_tl_asset(granule_id)] == xml


@pytest.mark.parametrize("valid", [True, False])
def test_check_solar_zenith(
    sentinel_config: EnvConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    valid: bool,
) -> None:
    """Tests that we handle valid or invalid solar zenith angle."""
    granule_id = "GRANULE_ID"
    validity = "valid" if valid else "invalid"

    binary = tmp_path / "check_solar_zenith_sentinel"
    binary.write_text(f"#!/bin/bash\necho {validity}")
    binary.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path), prepend=os.pathsep)

    task = CheckSolarZenith.map(granule_id)("check_sza")

    bundle = {mtd_tl_asset(granule_id): Path("MTD_TL.xml")}
    if valid:
        output = task.run(bundle)
        assert output[solar_valid_asset(granule_id)] is True
    else:
        with pytest.raises(TaskFailure) as ex_info:
            task.run(bundle)
        assert ex_info.value.exit_code == 3


def test_find_s2_footprint(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests finding and converting the S2 footprint."""
    granule_id = "GRANULE_ID"
    safe_dir = sentinel_config.working_dir / granule_id / f"{granule_id}.SAFE"
    qi_data = safe_dir / "GRANULE" / granule_id / "QI_DATA"
    qi_data.mkdir(parents=True, exist_ok=True)
    (qi_data / "MSK_DETFOO_B06.jp2").touch()

    task = FindS2Footprint.map(granule_id)(name="footprint")
    outputs = task.run({safe_dir_asset(granule_id): safe_dir, CONFIG: sentinel_config})

    detfoo = outputs[detfoo_file_asset(granule_id)]
    assert detfoo.exists()
    assert detfoo.suffix == ".bin"


def test_apply_s2_quality_mask(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests applying the quality mask."""
    granule_id = "GRANULE_ID"
    inner = (
        sentinel_config.working_dir / granule_id / "PRODUCT_ID" / "GRANULE" / granule_id
    )
    inner.mkdir(parents=True)

    task = ApplyS2QualityMask.map(granule_id)(name="mask")
    outputs = task.run({granule_dir_asset(granule_id): inner})

    assert outputs[quality_mask_applied_asset(granule_id)] is True


def test_derive_s2_angles(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests the final angle generation task."""
    granule_id = "GRANULE_ID"
    granule_dir = sentinel_config.working_dir / granule_id
    mtd_tl = granule_dir / "PRODUCT_ID" / "GRANULE" / granule_id / "MTD_TL.xml"
    mtd_tl.parent.mkdir(exist_ok=True, parents=True)
    mtd_tl.touch()

    detfoo = granule_dir / "detfoo.bin"
    detfoo.touch()

    task = DeriveS2Angles.map(granule_id)(name="angles")
    outputs = task.run(
        {
            CONFIG: sentinel_config,
            mtd_tl_asset(granule_id): mtd_tl,
            detfoo_file_asset(granule_id): detfoo,
            solar_valid_asset(granule_id): True,
            quality_mask_applied_asset(granule_id): True,
        }
    )

    assert outputs[angle_hdf_asset(granule_id)].exists()


def test_run_fmask_s2(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests Fmask execution and conversion."""
    granule_id = "GRANULE_ID"
    inner = sentinel_config.working_dir / granule_id / "GRANULE" / granule_id
    inner.mkdir(parents=True)
    # mock the TIF that fmask produces
    (inner / "foo_Fmask4.tif").touch()

    task = RunFmask.map(granule_id)(name="fmask")
    outputs = task.run({granule_dir_asset(granule_id): inner, CONFIG: sentinel_config})

    assert outputs[fmask_bin_asset(granule_id)].exists()


def test_process_hdf_parts(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests splitting ESPA output into HDF parts."""
    granule_id = "GRANULE_ID"
    granule_dir = sentinel_config.working_dir / granule_id
    granule_dir.mkdir(exist_ok=True, parents=True)

    xml = granule_dir / "test_espa.xml"
    xml.touch()

    # Needs LASRC output dir (parent of xml)
    lasrc_dir = xml.parent

    task = ProcessHdfParts.map(granule_id)(name="parts")
    # The mocks need to produce the output HDFs for them to be detected
    outputs = task.run(
        {
            CONFIG: sentinel_config,
            espa_xml_asset(granule_id): xml,
            lasrc_aerosol_qa_asset(granule_id): lasrc_dir,
        }
    )

    split_hdf_parts = outputs[split_hdf_parts_asset(granule_id)]
    assert len(split_hdf_parts) == 2
    assert split_hdf_parts[0].exists()


def test_combine_s2_hdf(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Test CombineS2Hdf"""
    granule_id = "GRANULE_ID"
    granule_dir = sentinel_config.working_dir / granule_id
    granule_dir.mkdir(exist_ok=True, parents=True)

    espa_xml = granule_dir / "ESPA_XML.xml"
    espa_xml.touch()

    split_hdf_parts = Paths()
    for part in ("1", "2"):
        split_hdf = granule_dir / "ESPA_ID_sr_{part}.hdf}"
        split_hdf_parts.append(split_hdf)

    task = CombineS2Hdf.map(granule_id)("CombineS2Hdf")

    output = task.run(
        {
            CONFIG: sentinel_config,
            espa_xml_asset(granule_id): espa_xml,
            split_hdf_parts_asset(granule_id): split_hdf_parts,
        }
    )

    assert output[combined_sr_hdf_asset(granule_id)].exists()


def test_add_fmask_sds(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    granule_id = "GRANULE_ID"
    granule_dir = sentinel_config.working_dir / granule_id
    granule_dir.mkdir(exist_ok=True, parents=True)

    sr_hdf = granule_dir / "ESPA_ID_sr_combined.hdf"
    sr_hdf.touch()
    lasrc_aerosol_qa = granule_dir / "ESPA_ID_aerosol_qa.img"
    lasrc_aerosol_qa.touch()
    fmask_bin = granule_dir / "fmask.bin"
    fmask_bin.touch()

    task = AddS2FmaskSds.map(granule_id)("AddS2FmaskSds")
    output = task.run(
        {
            CONFIG: sentinel_config,
            combined_sr_hdf_asset(granule_id): sr_hdf,
            lasrc_aerosol_qa_asset(granule_id): lasrc_aerosol_qa,
            fmask_bin_asset(granule_id): fmask_bin,
        }
    )

    assert output[final_sr_hdf_asset(granule_id)].exists()


def test_trim_s2_hdf(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    granule_id = "GRANULE_ID"
    granule_dir = sentinel_config.working_dir / granule_id
    granule_dir.mkdir(exist_ok=True, parents=True)

    final_sr = granule_dir / "sr.hdf"
    final_sr.touch()

    task = TrimS2Hdf.map(granule_id)("TrimS2Hdf")
    output = task.run({final_sr_hdf_asset(granule_id): final_sr})

    assert output[trimmed_hdf_asset(granule_id)].exists()
