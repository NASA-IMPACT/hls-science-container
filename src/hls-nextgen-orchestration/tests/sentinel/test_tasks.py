from __future__ import annotations

from pathlib import Path

from hls_nextgen_orchestration.base import AssetBundle
from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.sentinel.assets import (
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    CONSOLIDATED_ANGLE_HDF,
    CONSOLIDATED_SR_HDF,
    FINAL_OUTPUT_HDF,
    NBAR_INPUT_HDF,
    OUTPUT_BASE_NAME,
    RENAMED_ANGLE_HDF,
    RENAMED_HDF,
    RESAMPLED_HDF,
    SR_MANIFEST_FILE,
    EnvConfig,
    angle_hdf_asset,
    trimmed_hdf_asset,
)
from hls_nextgen_orchestration.sentinel.tasks import (
    ConsolidateGranules,
    ConvertToCogs,
    CreateManifest,
    CreateMetadata,
    RenameOutputs,
    Resample30m,
    sentinel_to_hls_granule,
    sentinel_to_nbar_hdf_filename,
)


def test_sentinel_to_hls_granule() -> None:
    """Tests the parsing logic for Sentinel granule IDs."""
    granule = Sentinel2Granule.from_str(
        "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
    )
    # 2020-01-01 is DOY 001. HMS is 102431 (from T102431)
    expected = "HLS.S30.T32TQM.2020001T102431.v2.0"
    assert sentinel_to_hls_granule(granule) == expected


def test_sentinel_to_nbar_hdf_filename() -> None:
    """Test intermediate NBAR filename reconstruction

    The sentinel-derive-nbar program derives the year and DOY
    from the filename, so if we get that wrong there'll be issues.
    """
    granule = Sentinel2Granule.from_str(
        "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
    )
    expected = "HLS.S30.T32TQM.2020001.102431.v2.0.hdf"
    assert sentinel_to_nbar_hdf_filename(granule) == expected


def test_ConsolidateGranules(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Test ConsolidateGranules works"""
    granule_ids = ["GRANULE_ID_1", "GRANULE_ID_2"]
    assets: AssetBundle = {CONFIG: sentinel_config}
    for granule_id in granule_ids:
        granule_dir = sentinel_config.working_dir / granule_id
        granule_dir.mkdir(exist_ok=True, parents=True)

        sr_path = granule_dir / "sr.hdf"
        sr_path.touch()
        assets[trimmed_hdf_asset(granule_id)] = sr_path

        angle_path = granule_dir / "angle.hdf"
        angle_path.touch()
        assets[angle_hdf_asset(granule_id)] = angle_path

    task = ConsolidateGranules.merge(granule_ids)("ConsolidateGranules")

    output = task.run(assets)
    for provided_asset in task.provides:
        assert provided_asset in output


def test_Resample30m(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests resampling to 30m."""
    input_hdf = sentinel_config.working_dir / "trimmed.hdf"
    input_hdf.touch()

    task = Resample30m(name="resample")
    outputs = task.run({CONSOLIDATED_SR_HDF: input_hdf, CONFIG: sentinel_config})

    # for non-debug mode we renamed the resampled HDF
    assert not outputs[RESAMPLED_HDF].exists()
    assert outputs[NBAR_INPUT_HDF].exists()


def test_RenameOutputs(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Verifies renaming of HDF files to Standard S30 format."""
    # Setup inputs
    original_hdf = sentinel_config.working_dir / "output.hdf"
    original_hdf.touch()
    original_angle = sentinel_config.working_dir / "angle.hdf"
    original_angle.touch()

    # Create dummy headers
    original_hdf.with_suffix(".hdf.hdr").touch()

    task = RenameOutputs(name="rename")
    bundle: AssetBundle = {
        CONFIG: sentinel_config,
        FINAL_OUTPUT_HDF: original_hdf,
        CONSOLIDATED_ANGLE_HDF: original_angle,
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


def test_ConvertToCogs(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests the HDF to COG conversion task."""
    hdf = sentinel_config.working_dir / "test.hdf"
    angle = sentinel_config.working_dir / "test.ANGLE.hdf"

    task = ConvertToCogs(name="cogs")
    outputs = task.run(
        {CONFIG: sentinel_config, RENAMED_HDF: hdf, RENAMED_ANGLE_HDF: angle}
    )

    assert outputs[COGS_CREATED] is True


def test_CreateMetadata(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests CMR and STAC metadata generation."""
    hdf = sentinel_config.working_dir / "test.hdf"
    base_name = "HLS.S30.TEST"

    task = CreateMetadata(name="meta")
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


def test_CreateManifest(sentinel_config: EnvConfig, mock_binaries: Path) -> None:
    """Tests manifest generation."""
    base_name = "HLS.S30.T32TQM.2020001T102431.v2.0"
    cmr = sentinel_config.working_dir / f"{base_name}.cmr.xml"

    task = CreateManifest(name="manifest")
    outputs = task.run(
        {CONFIG: sentinel_config, OUTPUT_BASE_NAME: base_name, CMR_XML: cmr}
    )

    assert outputs[SR_MANIFEST_FILE].exists()
    assert outputs[SR_MANIFEST_FILE].name == f"{base_name}.json"
