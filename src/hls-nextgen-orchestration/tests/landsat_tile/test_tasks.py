from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import PropertyMock, patch

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.base import TaskContext
from hls_nextgen_orchestration.landsat_tile.assets import (
    CONFIG,
    FINAL_HDF,
    FMASK_BIN,
    GRANULE_DIR,
    METADATA,
    MTL_FILE,
    SOLAR_VALID,
    UPLOAD_COMPLETE,
)
from hls_nextgen_orchestration.landsat_tile.tasks import (
    CheckSolarZenith,
    DownloadGranule,
    EnvConfig,
    EnvSource,
    ParseMetadata,
    RunFmask,
    UploadResults,
)

# --- Test Data Constants ---
JOB_ID = "job-123"
GRANULE = "LC08_L1TP_025030_20200101_20200114_01_T1"
BUCKET_IN = "input-bucket"
BUCKET_OUT = "output-bucket"


@pytest.fixture
def mock_config(tmp_path):
    """
    Creates an EnvConfig that points to a temporary directory
    instead of /var/scratch.
    """
    with patch(
        "hls_nextgen_orchestration.landsat_tile.tasks.EnvConfig.working_dir",
        new_callable=PropertyMock,
    ) as mock_wd:
        mock_wd.return_value = tmp_path / "scratch" / JOB_ID

        config = EnvConfig(
            job_id=JOB_ID,
            granule=GRANULE,
            input_bucket=BUCKET_IN,
            output_bucket=BUCKET_OUT,
            prefix="L8",
            ac_code="LaSRC",
        )
        # Ensure granule dir exists as EnvSource would
        if not config.granule_dir.exists():
            config.granule_dir.mkdir(parents=True)

        yield config


def test_env_source(monkeypatch, tmp_path):
    """Test environment variable parsing."""
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE)
    monkeypatch.setenv("INPUT_BUCKET", BUCKET_IN)
    monkeypatch.setenv("OUTPUT_BUCKET", BUCKET_OUT)

    # Patch Path inside the class property to avoid /var/scratch issues
    with patch(
        "hls_nextgen_orchestration.landsat_tile.tasks.EnvConfig.working_dir",
        new_callable=PropertyMock,
    ) as mock_wd:
        mock_wd.return_value = tmp_path / JOB_ID

        source = EnvSource("test_source", provides=(CONFIG,))
        result = source.fetch()

        assert CONFIG in result
        cfg = result[CONFIG]
        assert cfg.job_id == JOB_ID
        assert cfg.granule == GRANULE
        assert cfg.granule_dir.exists()


def test_download_granule(mock_binaries, mock_config, monkeypatch):
    """Test download calls binary and verifies output."""
    # Set GRANULE env var because our mock bash script uses it to name the MTL file
    monkeypatch.setenv("GRANULE", GRANULE)

    task = DownloadGranule(
        "test_download", requires=(CONFIG,), provides=(GRANULE_DIR, MTL_FILE)
    )

    ctx = TaskContext()
    ctx.put(CONFIG, mock_config)

    # We call run directly to avoid context boilerplate in execute()
    outputs = task.run({CONFIG: mock_config})

    assert outputs[GRANULE_DIR] == mock_config.granule_dir
    assert outputs[MTL_FILE].exists()
    assert outputs[MTL_FILE].name == f"{GRANULE}_MTL.txt"


def test_parse_metadata(mock_config):
    """Test metadata string parsing."""
    task = ParseMetadata("test_meta", requires=(CONFIG,), provides=(METADATA,))
    outputs = task.run({CONFIG: mock_config})

    meta = outputs[METADATA]
    # 20200101 -> 2020-01-01
    assert meta["year"] == "2020"
    assert meta["output_name"] == "2020-01-01_025030"
    assert meta["bucket_key"] == "2020-01-01/025030"


def test_check_solar_zenith_valid(mock_binaries, mock_config):
    """Test solar zenith check passes with valid mock."""
    mtl_path = mock_config.granule_dir / f"{GRANULE}_MTL.txt"
    mtl_path.touch()

    task = CheckSolarZenith("test_solar", requires=(MTL_FILE,), provides=(SOLAR_VALID,))
    outputs = task.run({MTL_FILE: mtl_path})
    assert outputs[SOLAR_VALID] is True


def test_run_fmask(mock_binaries, mock_config):
    """Test Fmask execution and file generation."""
    task = RunFmask("test_fmask", requires=(CONFIG, GRANULE_DIR), provides=(FMASK_BIN,))
    outputs = task.run({CONFIG: mock_config, GRANULE_DIR: mock_config.granule_dir})

    assert outputs[FMASK_BIN].exists()
    assert outputs[FMASK_BIN].name == "fmask.bin"


def test_upload_results(mock_aws_s3, mock_config):
    """Test production upload to S3."""
    s3: S3Client = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET_OUT)

    # Setup dummy files
    final_hdf = mock_config.granule_dir / "output.hdf"
    final_hdf.touch()
    (mock_config.granule_dir / "test_VAA.img").touch()

    meta = {"bucket_key": "2020/001", "output_name": "output"}

    task = UploadResults(
        "test_upload",
        requires=(CONFIG, METADATA, FINAL_HDF, GRANULE_DIR),
        provides=(UPLOAD_COMPLETE,),
    )
    outputs = task.run(
        {
            CONFIG: mock_config,
            METADATA: meta,
            FINAL_HDF: final_hdf,
            GRANULE_DIR: mock_config.granule_dir,
        }
    )

    assert outputs[UPLOAD_COMPLETE] is True

    # Check S3
    objs = s3.list_objects(Bucket=BUCKET_OUT)
    keys = [o["Key"] for o in objs.get("Contents", [])]
    assert "2020/001/output.hdf" in keys
    assert "2020/001/test_VAA.img" in keys
