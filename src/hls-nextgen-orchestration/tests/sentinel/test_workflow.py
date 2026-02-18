from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.sentinel.assets import RENAMED_HDF
from hls_nextgen_orchestration.sentinel.tasks import EnvSource, UploadAll
from hls_nextgen_orchestration.sentinel.workflow import construct_pipeline

# Test Constants
JOB_ID = "sentinel-workflow-test"
GRANULE_ID_1 = "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
GRANULE_ID_2 = "S2A_MSIL1C_20200101T123456_N0208_R065_T32TQM_20200101T123456"
GRANULE_IDS = [GRANULE_ID_1, GRANULE_ID_2]

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

    for granule_id in GRANULE_IDS:
        granule = Sentinel2Granule.from_str(granule_id)
        safe_zip = populate_sentinel_safe(tmp_path, granule)

        # Create a dummy zip file to "download"
        s3_client.upload_file(
            Filename=str(safe_zip), Bucket=IN_BUCKET, Key=f"{granule_id}.zip"
        )

    return s3_client


@pytest.fixture
def container_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Setup common envvars and working directory for this pretend container"""
    working_dir = tmp_path / "working"
    working_dir.mkdir()

    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", GIBS_BUCKET)
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(working_dir))

    return working_dir


def test_sentinel_pipeline_ordering(
    mock_binaries: Path,
    container_setup: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the ordering of the tasks within the workflow DAG"""
    granule_ids = ["ABC", "XYZ"]
    monkeypatch.setenv("GRANULE_LIST", ",".join(granule_ids))

    pipeline = construct_pipeline()

    # --- Pipeline start/end
    # Parsing of envvars is first
    assert isinstance(pipeline.execution_order[0], EnvSource)
    # Upload happens last
    assert isinstance(pipeline.execution_order[-1], UploadAll)

    # --- Verify parallel execution of each granule
    mapped_tasks: dict[str, dict[int, str]] = {
        granule_id: {} for granule_id in granule_ids
    }
    for idx, node in enumerate(pipeline.execution_order):
        node_name = node.__class__.__name__
        for granule_id in granule_ids:
            if node_name.endswith(granule_id):
                mapped_tasks[granule_id][idx] = node_name

    # Same count
    assert len(mapped_tasks[granule_ids[0]]) == len(mapped_tasks[granule_ids[1]])

    # Parallel, not interleaved
    if min(mapped_tasks[granule_ids[0]]) < min(mapped_tasks[granule_ids[1]]):
        assert max(mapped_tasks[granule_ids[0]]) < min(mapped_tasks[granule_ids[1]])
    else:
        assert min(mapped_tasks[granule_ids[0]]) > max(mapped_tasks[granule_ids[1]])


@pytest.mark.parametrize("granule_ids", [[GRANULE_ID_1], GRANULE_IDS])
def test_sentinel_pipeline_end_to_end(
    granule_ids: list[str],
    mock_binaries: Path,
    s3_setup: S3Client,
    container_setup: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end test for single and twin granules"""
    monkeypatch.setenv("GRANULE_LIST", ",".join(granule_ids))

    pipeline = construct_pipeline()

    # --- Run
    context = pipeline.run()
    assert context.exit_code == 0

    # --- Check local files
    assert RENAMED_HDF in context._store
    final_hdf_path = context._store[RENAMED_HDF]

    assert isinstance(final_hdf_path, Path)
    assert final_hdf_path.exists()

    # HLS.S30.T32TQM.2020001.001.hdf
    assert "HLS.S30.T32TQM" in final_hdf_path.name

    # Check upload
    uploaded_keys = [
        obj["Key"]
        for obj in s3_setup.list_objects_v2(
            Bucket=OUT_BUCKET,
        )["Contents"]
    ]
    # expected data are defined in the HDF_TO_COG fake script
    assert next(key for key in uploaded_keys if key.endswith("B05.tif"))
    assert next(key for key in uploaded_keys if key.endswith("VZA.tif"))
