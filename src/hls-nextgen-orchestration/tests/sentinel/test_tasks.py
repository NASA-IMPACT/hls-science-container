from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.base import AssetBundle, TaskFailure
from hls_nextgen_orchestration.sentinel.assets import (
    ANGLE_HDF,
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    DETFOO_FILE,
    ESPA_XML,
    FINAL_OUTPUT_HDF,
    FMASK_BIN,
    LASRC_OUTPUT_DIR,
    OUTPUT_BASE_NAME,
    QUALITY_MASK_APPLIED,
    RENAMED_ANGLE_HDF,
    RENAMED_HDF,
    RESAMPLED_HDF,
    SAFE_DIR,
    SAFE_GRANULE_INNER_DIR,
    SOLAR_VALID,
    SPLIT_HDF_PARTS,
    SR_MANIFEST_FILE,
    TRIMMED_HDF,
    EnvConfig,
)
from hls_nextgen_orchestration.sentinel.tasks import (
    ApplyS2QualityMask,
    CheckSolarZenith,
    DeriveS2Angles,
    DownloadSentinelGranule,
    FindS2Footprint,
    GetS2GranuleDir,
    ProcessHdfParts,
    RenameS2Outputs,
    Resample30m,
    RunS2Fmask,
    S2ConvertToCogs,
    S2CreateManifest,
    S2CreateMetadata,
    get_s30_output_name,
)


def test_download_sentinel(
    s3_client: S3Client, sentinel_config: EnvConfig, mock_binaries: Path
) -> None:
    """Tests that the granule is downloaded and unzipped."""
    s3_client.create_bucket(Bucket=sentinel_config.input_bucket)
    s3_client.put_object(
        Bucket=sentinel_config.input_bucket,
        Key=f"{sentinel_config.granule}.zip",
        Body=b"zip",
    )

    task = DownloadSentinelGranule(name="download")
    outputs = task.run({CONFIG: sentinel_config})

    assert SAFE_DIR in outputs
    assert outputs[SAFE_DIR].exists()


def test_get_granule_dir(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests finding the internal GRANULE directory."""
    # Setup structure
    safe_dir = sentinel_config.granule_dir / "test.SAFE"
    inner = safe_dir / "GRANULE" / "MY_GRANULE_ID"
    inner.mkdir(parents=True)

    task = GetS2GranuleDir(name="get_dir")
    outputs = task.run({SAFE_DIR: safe_dir})

    assert outputs[SAFE_GRANULE_INNER_DIR] == inner


def test_check_solar_zenith_fail(
    sentinel_config: EnvConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that invalid solar zenith angle raises TaskFailure."""
    bin_dir = tmp_path / "bad_bin"
    bin_dir.mkdir()
    (bin_dir / "sentinel-derive-angle").write_text("#!/bin/bash\nexit 3")
    (bin_dir / "sentinel-derive-angle").chmod(0o755)
    monkeypatch.setenv("PATH", str(bin_dir), prepend=os.pathsep)

    safe_dir = sentinel_config.granule_dir / "test.SAFE"
    task = CheckSolarZenith(name="solar")

    with pytest.raises(TaskFailure):
        task.run({SAFE_DIR: safe_dir})


def test_find_s2_footprint(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests finding and converting the S2 footprint."""
    safe_dir = sentinel_config.granule_dir / "test.SAFE"
    qi_data = safe_dir / "GRANULE" / "MY_GRANULE_ID" / "QI_DATA"
    qi_data.mkdir(parents=True, exist_ok=True)
    (qi_data / "MSK_DETFOO_B06.jp2").touch()

    task = FindS2Footprint(name="footprint")
    outputs = task.run({SAFE_DIR: safe_dir, CONFIG: sentinel_config})

    assert DETFOO_FILE in outputs
    assert outputs[DETFOO_FILE].exists()
    assert outputs[DETFOO_FILE].suffix == ".bin"


def test_apply_s2_quality_mask(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests applying the quality mask."""
    inner = sentinel_config.granule_dir / "test.SAFE" / "GRANULE" / "ID"
    inner.mkdir(parents=True)

    task = ApplyS2QualityMask(name="mask")
    outputs = task.run({SAFE_GRANULE_INNER_DIR: inner})

    assert outputs[QUALITY_MASK_APPLIED] is True


def test_derive_s2_angles(
    sentinel_config: EnvConfig,
    mock_binaries: Path,
    populate_sentinel_safe: Callable[[EnvConfig], Path],
) -> None:
    """Tests the final angle generation task."""
    safe_dir = populate_sentinel_safe(sentinel_config)

    detfoo = sentinel_config.granule_dir / "detfoo.bin"
    detfoo.touch()

    task = DeriveS2Angles(name="angles")
    outputs = task.run(
        {
            SAFE_GRANULE_INNER_DIR: safe_dir,
            CONFIG: sentinel_config,
            SOLAR_VALID: True,
            DETFOO_FILE: detfoo,
            QUALITY_MASK_APPLIED: True,
        }
    )

    assert outputs[ANGLE_HDF].exists()


def test_run_fmask_s2(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests Fmask execution and conversion."""
    inner = sentinel_config.granule_dir / "GRANULE" / "ID"
    inner.mkdir(parents=True)
    # mock the TIF that fmask produces
    (inner / "foo_Fmask4.tif").touch()

    task = RunS2Fmask(name="fmask")
    outputs = task.run({SAFE_GRANULE_INNER_DIR: inner, CONFIG: sentinel_config})

    assert outputs[FMASK_BIN].exists()


def test_process_hdf_parts(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests splitting ESPA output into HDF parts."""
    xml = sentinel_config.granule_dir / "test_espa.xml"
    xml.touch()

    # Needs LASRC output dir (parent of xml)
    lasrc_dir = xml.parent

    task = ProcessHdfParts(name="parts")
    # The mocks need to produce the output HDFs for them to be detected
    outputs = task.run(
        {ESPA_XML: xml, CONFIG: sentinel_config, LASRC_OUTPUT_DIR: lasrc_dir}
    )

    assert len(outputs[SPLIT_HDF_PARTS]) == 2
    assert outputs[SPLIT_HDF_PARTS][0].exists()


def test_resample(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests resampling to 30m."""
    inp = sentinel_config.granule_dir / "trimmed.hdf"
    inp.touch()

    task = Resample30m(name="resample")
    outputs = task.run({TRIMMED_HDF: inp, CONFIG: sentinel_config})

    assert outputs[RESAMPLED_HDF].exists()


def test_get_s30_output_name() -> None:
    """Tests the parsing logic for Sentinel granule IDs."""
    granule = "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
    # 2020-01-01 is DOY 001. HMS is 102431 (from T102431)
    expected = "HLS.S30.T32TQM.2020001T102431.v2.0"
    assert get_s30_output_name(granule) == expected


def test_rename_outputs(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Verifies renaming of HDF files to Standard S30 format."""
    # Setup inputs
    original_hdf = sentinel_config.granule_dir / "output.hdf"
    original_hdf.touch()
    original_angle = sentinel_config.granule_dir / "angle.hdf"
    original_angle.touch()

    # Create dummy headers
    original_hdf.with_suffix(".hdf.hdr").touch()

    task = RenameS2Outputs(name="rename")
    bundle: AssetBundle = {
        CONFIG: sentinel_config,
        FINAL_OUTPUT_HDF: original_hdf,
        ANGLE_HDF: original_angle,
    }

    outputs = task.run(bundle)

    base_name = outputs[OUTPUT_BASE_NAME]
    assert "HLS.S30" in base_name

    renamed_hdf = outputs[RENAMED_HDF]
    assert renamed_hdf.exists()
    assert renamed_hdf.name == f"{base_name}.hdf"
    # Check header moved
    assert renamed_hdf.with_suffix(".hdf.hdr").exists()

    # Original should be gone
    assert not original_hdf.exists()


def test_convert_to_cogs(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests the HDF to COG conversion task."""
    hdf = sentinel_config.granule_dir / "test.hdf"
    angle = sentinel_config.granule_dir / "test.ANGLE.hdf"

    task = S2ConvertToCogs(name="cogs")
    outputs = task.run(
        {CONFIG: sentinel_config, RENAMED_HDF: hdf, RENAMED_ANGLE_HDF: angle}
    )

    assert outputs[COGS_CREATED] is True


def test_create_metadata(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests CMR and STAC metadata generation."""
    hdf = sentinel_config.granule_dir / "test.hdf"
    base_name = "HLS.S30.TEST"

    task = S2CreateMetadata(name="meta")
    outputs = task.run(
        {
            CONFIG: sentinel_config,
            RENAMED_HDF: hdf,
            OUTPUT_BASE_NAME: base_name,
            COGS_CREATED: True,
        }
    )

    assert outputs[CMR_XML].exists()
    assert outputs[CMR_XML].name == f"{base_name}.cmr.xml"


def test_create_manifest(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests manifest generation."""
    base_name = "HLS.S30.T32TQM.2020001T102431.v2.0"
    cmr = sentinel_config.working_dir / f"{base_name}.cmr.xml"

    task = S2CreateManifest(name="manifest")
    outputs = task.run(
        {CONFIG: sentinel_config, OUTPUT_BASE_NAME: base_name, CMR_XML: cmr}
    )

    assert outputs[SR_MANIFEST_FILE].exists()
    assert outputs[SR_MANIFEST_FILE].name == f"{base_name}.json"
