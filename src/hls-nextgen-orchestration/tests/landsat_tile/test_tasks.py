from __future__ import annotations

import datetime as dt
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.landsat_tile.assets import (
    ANGLE_HDF,
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    GIBS_DIR,
    GRIDDED_HDF,
    SR_MANIFEST_FILE,
    VI_MANIFEST_FILE,
    GIBS_MANIFEST_FILES,
    NBAR_ANGLE,
    NBAR_INPUT,
    OUTPUT_BASE_NAME,
    OUTPUT_HDF,
    SCENE_TIME,
    STAC_JSON,
    THUMBNAIL_FILE,
    UPLOAD_COMPLETE,
    VI_DIR,
    EnvConfig,
)
from hls_nextgen_orchestration.landsat_tile.tasks import (
    ConvertToCogs,
    CreateSRManifest,
    CreateMetadata,
    CreateThumbnail,
    DownloadPathRows,
    EnvSource,
    ProcessGibs,
    ProcessPathRows,
    ProcessVi,
    RunNbar,
    UploadAll,
)

# --- Test Data Constants ---
JOB_ID = "job-tile-123"
PATHROW_LIST = "025030"
DATE = dt.date(2020, 1, 1)
MGRS = "12ABC"
MGRS_ULX = "100"
MGRS_ULY = "200"
BUCKET_IN = "input-bucket"
BUCKET_OUT = "output-bucket"
BUCKET_GIBS = "gibs-bucket"

# Mocked output of extract_landsat_hms.py
MOCKED_SCENE_TIME = "101010"


@pytest.fixture
def mock_config(tmp_path: Path) -> Generator[EnvConfig, None, None]:
    """
    Creates an EnvConfig that points to a temporary directory.
    """
    config = EnvConfig(
        job_id=JOB_ID,
        pathrow_list=PATHROW_LIST.split(","),
        date=DATE,
        mgrs=MGRS,
        mgrs_ulx=MGRS_ULX,
        mgrs_uly=MGRS_ULY,
        input_bucket=BUCKET_IN,
        output_bucket=BUCKET_OUT,
        gibs_bucket=BUCKET_GIBS,
        working_dir=tmp_path,
    )
    if not config.working_dir.exists():
        config.working_dir.mkdir(parents=True)

    yield config


def test_env_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test environment variable parsing."""
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("PATHROW_LIST", PATHROW_LIST)
    monkeypatch.setenv("DATE", DATE.isoformat())
    monkeypatch.setenv("MGRS", MGRS)
    monkeypatch.setenv("MGRS_ULX", MGRS_ULX)
    monkeypatch.setenv("MGRS_ULY", MGRS_ULY)
    monkeypatch.setenv("INPUT_BUCKET", BUCKET_IN)
    monkeypatch.setenv("OUTPUT_BUCKET", BUCKET_OUT)
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", BUCKET_GIBS)
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))

    source = EnvSource("test_source")
    result = source.fetch()

    assert CONFIG in result
    cfg = result[CONFIG]
    assert cfg.job_id == JOB_ID
    assert cfg.pathrow_list == ["025030"]
    assert cfg.date == DATE
    assert cfg.mgrs == MGRS
    assert cfg.working_dir.exists()


def test_download_pathrows(mock_config: EnvConfig, mock_aws_s3: S3Client) -> None:
    """
    Test downloading Landsat atmospheric correction path/rows
    """
    # Setup S3 input data
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET_IN)

    # Prefix based on config date/pathrow: 2020-01-01/025030
    prefix = "2020-01-01/025030"

    # Create dummy files that would exist in the source bucket
    # landsat_ac name format derived in task: {date}_{pathrow}.hdf
    ac_file = "2020-01-01_025030.hdf"
    sza_file = "2020-01-01_025030_SZA.img"

    s3.put_object(Bucket=BUCKET_IN, Key=f"{prefix}/{ac_file}", Body="dummy content")
    s3.put_object(Bucket=BUCKET_IN, Key=f"{prefix}/{sza_file}", Body="dummy content")

    task = DownloadPathRows("test_process")
    task.run({CONFIG: mock_config})

    # Verify files were downloaded to working dir via boto3
    assert (mock_config.working_dir / ac_file).exists()
    assert (mock_config.working_dir / sza_file).exists()


def process_path_rows(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """
    Test running landsat-tile tools. Should produce NBAR inputs and scene time.
    """
    task = ProcessPathRows("test_process")

    # We mock subprocess.run only for the tools,
    # letting mock_binaries handle side effects.
    with patch("subprocess.run") as mock_run:
        # Configure side effects for the mocked binaries
        def side_effect(cmd: str, **kwargs: Any) -> MagicMock:
            if cmd[0] == "extract_landsat_hms.py":
                # Mock return of scene time
                return MagicMock(stdout=f"{MOCKED_SCENE_TIME}\n")
            elif cmd[0] == "landsat-tile":
                # Simulate output creation
                # cmd[-1] is output nbar input hdf
                Path(cmd[-1]).touch()
                return MagicMock(returncode=0)
            elif cmd[0] == "landsat-angle-tile":
                # cmd[-1] is output nbar angle hdf
                Path(cmd[-1]).touch()
                return MagicMock(returncode=0)
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect

        outputs = task.run({CONFIG: mock_config})

        # Expected ID components
        # Full ID uses 'T' separator for time: T123456
        full_id = f"HLS.L30.T{MGRS}.2020001T{MOCKED_SCENE_TIME}.v2.0"
        # Legacy/NBAR input uses '.' separator for time: 123456
        nbar_legacy_time = MOCKED_SCENE_TIME
        nbar_legacy_base = f"{MGRS}.2020001.{nbar_legacy_time}.v2.0"

        # Verify assets produced
        assert outputs[SCENE_TIME] == MOCKED_SCENE_TIME
        # Check that NBAR input uses legacy naming with dots
        assert outputs[NBAR_INPUT].name == f"HLS.L30.{nbar_legacy_base}.hdf"
        assert outputs[NBAR_ANGLE].name == f"L8ANGLE.{nbar_legacy_base}.hdf"

        # Output Base Name should be the full standard HLS ID
        assert outputs[OUTPUT_BASE_NAME] == full_id


def test_run_nbar(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test NBAR execution and renaming."""
    scene_time = MOCKED_SCENE_TIME
    # Use a valid HLS ID for the input, as RunNbar now parses it
    full_id = f"HLS.L30.T{MGRS}.2020001T{scene_time}.v2.0"
    output_basename = full_id

    # Setup Inputs using Legacy naming (dots)
    nbar_legacy_base = f"{MGRS}.2020001.{scene_time}.v2.0"
    nbar_input = mock_config.working_dir / f"HLS.L30.{nbar_legacy_base}.hdf"
    nbar_input.touch()
    nbar_angle = mock_config.working_dir / f"L8ANGLE.{nbar_legacy_base}.hdf"
    nbar_angle.touch()

    task = RunNbar("test_nbar")

    outputs = task.run(
        {
            CONFIG: mock_config,
            NBAR_INPUT: nbar_input,
            NBAR_ANGLE: nbar_angle,
            SCENE_TIME: scene_time,
            OUTPUT_BASE_NAME: output_basename,
        }
    )

    # Check outputs were renamed/created to the Full ID
    expected_output = mock_config.working_dir / f"{full_id}.hdf"
    expected_angle = mock_config.working_dir / f"{full_id}.ANGLE.hdf"

    assert outputs[OUTPUT_HDF] == expected_output
    assert outputs[ANGLE_HDF] == expected_angle
    assert outputs[GRIDDED_HDF].exists()  # Copied for debug

    # Original files should be moved
    assert not nbar_input.exists()
    assert expected_output.exists()


def test_convert_to_cogs(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test COG conversion calls."""
    output_hdf = mock_config.working_dir / "output.hdf"
    angle_hdf = mock_config.working_dir / "angle.hdf"
    output_hdf.touch()
    angle_hdf.touch()

    task = ConvertToCogs("test_cogs")

    outputs = task.run(
        {CONFIG: mock_config, OUTPUT_HDF: output_hdf, ANGLE_HDF: angle_hdf}
    )

    assert outputs[COGS_CREATED] is True


def test_create_thumbnail(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test thumbnail generation."""
    # Use full ID
    output_basename = f"HLS.L30.T{MGRS}.2020001T101010.v2.0"

    task = CreateThumbnail("test_thumb")

    outputs = task.run({CONFIG: mock_config, OUTPUT_BASE_NAME: output_basename})

    # Task now uses basename directly
    assert outputs[THUMBNAIL_FILE].name == f"{output_basename}.jpg"
    assert outputs[THUMBNAIL_FILE].exists()


def test_create_metadata(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test CMR/STAC metadata generation."""
    output_hdf = mock_config.working_dir / "output.hdf"
    output_basename = f"HLS.L30.T{MGRS}.2020001T101010.v2.0"

    task = CreateMetadata("test_meta")

    outputs = task.run(
        {CONFIG: mock_config, OUTPUT_HDF: output_hdf, OUTPUT_BASE_NAME: output_basename}
    )

    # Task now uses basename directly
    assert outputs[CMR_XML].name == f"{output_basename}.cmr.xml"
    assert outputs[STAC_JSON].name == f"{output_basename}_stac.json"
    assert outputs[CMR_XML].exists()
    assert outputs[STAC_JSON].exists()


def test_create_manifest(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test surface reflectance manifest creation."""
    output_basename = f"HLS.L30.T{MGRS}.2020001T101010.v2.0"

    task = CreateSRManifest("test_manifest")

    outputs = task.run(
        {
            CONFIG: mock_config,
            OUTPUT_BASE_NAME: output_basename,
            COGS_CREATED: True,
            THUMBNAIL_FILE: MagicMock(),
            CMR_XML: MagicMock(),
        }
    )

    assert outputs[SR_MANIFEST_FILE].name == f"{output_basename}.json"
    assert outputs[SR_MANIFEST_FILE].exists()


def test_process_gibs(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test GIBS processing and sub-manifest creation."""
    output_basename = "HLS.L30.TESTID.v2.0"

    task = ProcessGibs("test_gibs")

    outputs = task.run({CONFIG: mock_config, OUTPUT_BASE_NAME: output_basename})

    gibs_dir = outputs[GIBS_DIR]
    assert gibs_dir.exists()
    # The mock binary creates a subdir "GIBS_ID_1" with "test.xml"
    assert (gibs_dir / "GIBS_ID_1" / "test.xml").exists()


def test_process_vi(mock_binaries: Path, mock_config: EnvConfig) -> None:
    """Test VI processing."""
    output_basename = "HLS.L30.TESTID.v2.0"

    task = ProcessVi("test_vi")

    outputs = task.run({CONFIG: mock_config, OUTPUT_BASE_NAME: output_basename})

    vi_dir = outputs[VI_DIR]
    assert vi_dir.exists()
    # Check for mocked artifacts
    assert (vi_dir / "NDVI.tif").exists()


def test_upload_all_production(mock_binaries: Path, mock_aws_s3: S3Client, mock_config: EnvConfig) -> None:
    """Test production upload logic."""
    s3: S3Client = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET_OUT)
    s3.create_bucket(Bucket=BUCKET_GIBS)

    granule_id = f"HLS.L30.T{MGRS}.2020001T101010.v2.0"
    vi_id = granule_id.replace("HLS.L30", "HLS-VI.L30")

    # Create dummy files to upload
    (mock_config.working_dir / "product.tif").touch()
    (mock_config.working_dir / f"{granule_id}.json").touch()  # Manifest

    # Dummy GIBS
    gibs_dir = mock_config.working_dir / "gibs"
    gibs_manifest_files = []
    for gibs_id in ["id1"]:
        (gibs_dir / gibs_id).mkdir(parents=True)
        (gibs_dir / gibs_id / "gibs.tif").touch()
        (gibs_dir / gibs_id / "gibs.xml").touch()
        gibs_manifest_file = (gibs_dir / gibs_id / "gibs.json")
        gibs_manifest_file.touch()
        gibs_manifest_files.append(gibs_manifest_file)

    # Dummy VI
    vi_dir = mock_config.working_dir / "vi"
    vi_dir.mkdir()
    (vi_dir / "vi.tif").touch()
    (vi_dir / f"{vi_id}.json").touch()

    task = UploadAll("test_upload")

    outputs = task.run(
        {
            CONFIG: mock_config,
            OUTPUT_BASE_NAME: granule_id,
            GIBS_DIR: gibs_dir,
            GIBS_MANIFEST_FILES: gibs_manifest_files,
            GRIDDED_HDF: Path("dummy"),
            SR_MANIFEST_FILE: mock_config.working_dir / f"{granule_id}.json",
            VI_DIR: vi_dir,
            VI_MANIFEST_FILE: mock_config.working_dir / f"{granule_id}_vi.json",
        }
    )

    assert outputs[UPLOAD_COMPLETE] is True

    # Verify S3 Contents
    # 1. Main Product
    # Key structure: L30/data/2020001/{granule_id}/product.tif
    main_objs = s3.list_objects(
        Bucket=BUCKET_OUT, Prefix=f"L30/data/2020001/{granule_id}"
    )
    main_keys = [o["Key"] for o in main_objs.get("Contents", [])]
    assert any("product.tif" in k for k in main_keys)
    # Check manifest upload
    assert any(f"{granule_id}.json" in k for k in main_keys)

    # 2. GIBS
    # Key: L30/data/2020001/id1/gibs.tif
    gibs_objs = s3.list_objects(Bucket=BUCKET_GIBS, Prefix="L30/data/2020001")
    gibs_keys = [o["Key"] for o in gibs_objs.get("Contents", [])]
    assert any("id1/gibs.tif" in k for k in gibs_keys)

    # 3. VI
    # Key: L30_VI/data/2020001/{vi_id}/vi.tif
    vi_objs = s3.list_objects(Bucket=BUCKET_OUT, Prefix=f"L30_VI/data/2020001/{vi_id}")
    vi_keys = [o["Key"] for o in vi_objs.get("Contents", [])]
    assert any("vi.tif" in k for k in vi_keys)
    # Check manifest upload
    assert any(f"{vi_id}.json" in k for k in vi_keys)
