from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import PropertyMock, patch

import pytest

from hls_nextgen_orchestration.landsat_tile.assets import UPLOAD_COMPLETE
from hls_nextgen_orchestration.landsat_tile.workflow import construct_pipeline

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

# Constants
JOB_ID = "workflow-job-1"
GRANULE = "LC08_L1TP_025030_20200101_20200114_01_T1"
IN_BUCKET = "landsat-pds"
OUT_BUCKET = "processed-data"


@pytest.fixture
def s3_setup(s3_client: S3Client) -> S3Client:
    s3_client.create_bucket(Bucket=IN_BUCKET)
    s3_client.create_bucket(Bucket=OUT_BUCKET)
    return s3_client


def test_pipeline_end_to_end(mock_binaries, s3_setup: S3Client, monkeypatch, tmp_path):
    """
    Runs the full landsat tile pipeline using mocked binaries and mocked S3.
    """
    # 1. Setup Environment
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("PREFIX", "L8")
    monkeypatch.setenv("ACCODE", "LaSRC")

    # 2. Patch Working Directory
    # We must patch the EnvConfig property used inside the tasks
    with patch(
        "hls_nextgen_orchestration.landsat_tile.tasks.EnvConfig.working_dir",
        new_callable=PropertyMock,
    ) as mock_wd:
        mock_wd.return_value = tmp_path / "scratch" / JOB_ID

        # 3. Construct Pipeline
        pipeline = construct_pipeline()

        # 4. Run Pipeline
        context = pipeline.run()

        # 5. Verify Success
        assert context.get(UPLOAD_COMPLETE) is True

        # 6. Verify Side Effects (S3 Upload)
        s3 = s3_setup
        # Metadata parsing logic:
        # Date: 20200101 -> 2020-01-01
        # PathRow: 025030
        expected_key = "2020-01-01/025030/2020-01-01_025030.hdf"

        objs = s3.list_objects(Bucket=OUT_BUCKET, Prefix=expected_key)
        assert "Contents" in objs
        assert objs["Contents"][0]["Key"] == expected_key
