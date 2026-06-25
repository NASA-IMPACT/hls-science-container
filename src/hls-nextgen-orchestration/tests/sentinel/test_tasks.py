from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from hls_nextgen_orchestration.base import AssetBundle
from hls_nextgen_orchestration.common.utils import run_command
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
    THUMBNAIL_FILE,
    EnvConfig,
    angle_hdf_asset,
    trimmed_hdf_asset,
)
from hls_nextgen_orchestration.sentinel.tasks import (
    ConsolidateGranules,
    ConvertToCogs,
    CreateManifest,
    CreateMetadata,
    ProcessGibs,
    ProcessVi,
    RenameOutputs,
    Resample30m,
    sentinel_to_nbar_hdf_filename,
)


def _create_manifest_bucket_keys(spy: MagicMock) -> list[str]:
    """Collect the bucket_key argument from every create_manifest invocation.

    ``create_manifest`` is called as
    ``[create_manifest, <dir>, <manifest>, <bucket_key>, ...]`` so the
    bucket_key lives at index 3 of the command list.
    """
    keys = []
    for call in spy.call_args_list:
        cmd = call.args[0]
        if cmd and cmd[0] == "create_manifest":
            keys.append(cmd[3])
    return keys


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


# --- create_manifest bucket_key regression tests
#
# Regression: create_manifest for the VI and GIBS products was handed a bare
# key (e.g. "S30/data/2020001/...") with no "s3://bucket/" scheme, because the
# *_bucket_prefix properties returned keys and the call sites forgot to prepend
# the bucket. The manifest argument must always be a full s3:// URI.
def test_CreateManifest_passes_full_s3_uri(
    sentinel_config: EnvConfig, mock_binaries: Path
) -> None:
    """Main product manifest must reference s3://<output_bucket>/..."""
    base_name = "HLS.S30.T32TQM.2020001T102431.v2.0"
    cmr = sentinel_config.working_dir / f"{base_name}.cmr.xml"

    with patch(
        "hls_nextgen_orchestration.sentinel.tasks.run_command", wraps=run_command
    ) as spy:
        CreateManifest(name="manifest").run(
            {CONFIG: sentinel_config, OUTPUT_BASE_NAME: base_name, CMR_XML: cmr}
        )

    keys = _create_manifest_bucket_keys(spy)
    assert keys, "create_manifest was never called"
    for key in keys:
        assert key.startswith(f"s3://{sentinel_config.output_bucket}/"), key


def test_ProcessGibs_manifest_passes_full_s3_uri(
    sentinel_config: EnvConfig, mock_binaries: Path
) -> None:
    """GIBS sub-tile manifests must reference s3://<gibs_bucket>/... (not a bare key)."""
    base_name = "HLS.S30.T32TQM.2020001T102431.v2.0"

    with patch(
        "hls_nextgen_orchestration.sentinel.tasks.run_command", wraps=run_command
    ) as spy:
        ProcessGibs(name="gibs").run(
            {
                CONFIG: sentinel_config,
                OUTPUT_BASE_NAME: base_name,
                SR_MANIFEST_FILE: sentinel_config.working_dir / f"{base_name}.json",
            }
        )

    keys = _create_manifest_bucket_keys(spy)
    assert keys, "create_manifest was never called for any GIBS sub-tile"
    for key in keys:
        assert key.startswith(f"s3://{sentinel_config.gibs_bucket}/"), key


def test_ProcessVi_manifest_passes_full_s3_uri(
    sentinel_config: EnvConfig, mock_binaries: Path
) -> None:
    """VI manifest must reference s3://<output_bucket>/... (not a bare key)."""
    with patch(
        "hls_nextgen_orchestration.sentinel.tasks.run_command", wraps=run_command
    ) as spy:
        ProcessVi(name="vi").run(
            {
                CONFIG: sentinel_config,
                SR_MANIFEST_FILE: sentinel_config.working_dir / "manifest.json",
                THUMBNAIL_FILE: sentinel_config.working_dir / "thumb.jpg",
            }
        )

    keys = _create_manifest_bucket_keys(spy)
    assert keys, "create_manifest was never called for the VI product"
    for key in keys:
        assert key.startswith(f"s3://{sentinel_config.output_bucket}/"), key
