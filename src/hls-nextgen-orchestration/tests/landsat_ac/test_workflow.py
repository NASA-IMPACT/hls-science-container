from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from hls_nextgen_orchestration.landsat_ac.assets import UPLOAD_COMPLETE
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
