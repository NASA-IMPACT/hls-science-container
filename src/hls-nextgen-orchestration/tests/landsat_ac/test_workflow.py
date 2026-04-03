from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from hls_nextgen_orchestration.constants import FMASK_VERSION
from hls_nextgen_orchestration.landsat_ac.assets import UPLOAD_COMPLETE
from hls_nextgen_orchestration.landsat_ac.tasks import RunFmask, RunFmaskV5
from hls_nextgen_orchestration.landsat_ac.workflow import construct_pipeline

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

JOB_ID = "workflow-job-2"
GRANULE = "LC08_L1TP_025030_20200101_20200114_01_T1"
IN_BUCKET = "usgs-landsat"
OUT_BUCKET = "processed-data"


@pytest.fixture
def s3_setup(s3_client: S3Client) -> S3Client:
    s3_client.create_bucket(Bucket=IN_BUCKET)
    s3_client.create_bucket(Bucket=OUT_BUCKET)
    return s3_client


@pytest.mark.parametrize(
    ("fmask_version", "expected_task_cls"),
    [("v4", RunFmask), ("v5", RunFmaskV5)],
)
def test_landsat_pipeline_fmask_toggle(
    mock_binaries: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fmask_version: FMASK_VERSION,
    expected_task_cls: type,
) -> None:
    """Verify that the correct Fmask task class is used based on fmask_version."""
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("PREFIX", "L30")
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))

    pipeline = construct_pipeline(fmask_version=fmask_version)
    fmask_tasks = [
        t for t in pipeline.execution_order if isinstance(t, (RunFmask, RunFmaskV5))
    ]

    assert len(fmask_tasks) == 1
    assert isinstance(fmask_tasks[0], expected_task_cls)


def test_landsat_pipeline_end_to_end(
    mock_binaries: Path,
    s3_setup: S3Client,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("PREFIX", "L30")
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))

    pipeline = construct_pipeline()
    context = pipeline.run()

    assert context.get(UPLOAD_COMPLETE) is True
