from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import PropertyMock, patch

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
    mock_binaries, s3_setup: S3Client, monkeypatch, tmp_path
):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("PREFIX", "L8")
    monkeypatch.setenv("ACCODE", "LaSRC")

    with patch(
        "hls_nextgen_orchestration.landsat_ac.tasks.EnvConfig.working_dir",
        new_callable=PropertyMock,
    ) as mock_wd:
        mock_wd.return_value = tmp_path / "scratch" / JOB_ID

        pipeline = construct_pipeline()
        context = pipeline.run()

        assert context.get(UPLOAD_COMPLETE) is True
