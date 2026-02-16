from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.sentinel.assets import RENAMED_HDF, RENAMED_ANGLE_HDF
from hls_nextgen_orchestration.sentinel.workflow import construct_pipeline

# Test Constants
JOB_ID = "sentinel-workflow-test"
GRANULE_ID = "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"

IN_BUCKET = "hls-sentinel-inputs"
OUT_BUCKET = "hls-products"
GIBS_BUCKET = "hls-gibs-bucket"


@pytest.fixture
def s3_setup(
    tmp_path: Path,
    s3_client: S3Client,
    populate_sentinel_safe: Callable[[Path, Sentinel2Granule], Path],
) -> S3Client:
    """Setup mock S3 buckets and initial data using root moto fixture."""
    s3_client.create_bucket(Bucket=IN_BUCKET)
    s3_client.create_bucket(Bucket=OUT_BUCKET)
    s3_client.create_bucket(Bucket=GIBS_BUCKET)

    granule = Sentinel2Granule.from_str(GRANULE_ID)
    safe_zip = populate_sentinel_safe(tmp_path, granule)

    # Create a dummy zip file to "download"
    s3_client.upload_file(
        Filename=str(safe_zip), Bucket=IN_BUCKET, Key=f"{GRANULE_ID}.zip"
    )
    return s3_client


def test_sentinel_pipeline_end_to_end(
    mock_binaries: Path,
    s3_setup: S3Client,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    End-to-end test of the Sentinel-2 pipeline using root conftest mocks.
    """
    # 1. Setup Environment
    working_dir = tmp_path / "working"
    working_dir.mkdir()

    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE_ID)
    monkeypatch.setenv("PREFIX", "S30")
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", GIBS_BUCKET)
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(working_dir))

    # 2. Construct Pipeline
    pipeline = construct_pipeline()

    # 3. Execute Pipeline
    context = pipeline.run()
    assert context.exit_code == 0

    # 4. Assertions
    assert RENAMED_HDF in context._store
    final_hdf_path = context._store[RENAMED_HDF]

    assert isinstance(final_hdf_path, Path)
    assert final_hdf_path.exists()

    # Verify the naming convention derived from ParseMetadata (via SentinelGranule)
    # HLS.S30.T32TQM.2020001.001.hdf
    assert "HLS.S30.T32TQM" in final_hdf_path.name


def test_construct_pipeline_structure(mock_binaries: Path) -> None:
    """Verify the pipeline builder creates a valid DAG."""
    pipeline = construct_pipeline()
    assert len(pipeline.execution_order) >= 5
    first_task = pipeline.execution_order[0]
    assert "Metadata" in first_task.name or "Env" in first_task.name
