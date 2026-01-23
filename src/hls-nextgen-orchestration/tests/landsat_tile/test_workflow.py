from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import PropertyMock, patch

import pytest

from hls_nextgen_orchestration.landsat_tile.assets import UPLOAD_COMPLETE
from hls_nextgen_orchestration.landsat_tile.workflow import construct_pipeline

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

# Constants
JOB_ID = "job-tile-1"
PATHROW_LIST = "025030"
DATE = "2020-01-01"
MGRS = "12ABC"  # Fixed: No 'T' prefix here, pipeline adds it
IN_BUCKET = "input-bucket"
OUT_BUCKET = "output-bucket"
GIBS_BUCKET = "gibs-bucket"


@pytest.fixture
def s3_setup(s3_client: S3Client) -> S3Client:
    s3_client.create_bucket(Bucket=IN_BUCKET)
    s3_client.create_bucket(Bucket=OUT_BUCKET)
    s3_client.create_bucket(Bucket=GIBS_BUCKET)

    # Create dummy input data
    # Key: {year}-{month}-{day}/{pathrow}/{date}_{pathrow}.hdf
    input_key = "2020-01-01/025030/2020-01-01_025030.hdf"
    s3_client.put_object(Bucket=IN_BUCKET, Key=input_key, Body="dummy")

    return s3_client


def test_pipeline_end_to_end(mock_binaries, s3_setup: S3Client, monkeypatch, tmp_path):
    """
    Runs the full landsat tile pipeline using mocked binaries and mocked S3.
    """
    # 1. Setup Environment
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("PATHROW_LIST", PATHROW_LIST)
    monkeypatch.setenv("DATE", DATE)
    monkeypatch.setenv("MGRS", MGRS)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", GIBS_BUCKET)

    # 2. Patch Working Directory
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
        assert context.exit_code == 0
        assert context.get(UPLOAD_COMPLETE) is True

        # 6. Verify Side Effects (S3 Upload)
        # Expected Output: L30/data/2020{DOY}/HLS.L30.T{MGRS}.2020{DOY}T{HMS}.v2.0/
        # DOY for Jan 1 = 001. HMS from mock = 101010
        # Pipeline logic: f"T{config.mgrs}..." so T12ABC
        expected_prefix = f"L30/data/2020001/HLS.L30.T{MGRS}.2020001T101010.v2.0"

        objs = s3_setup.list_objects(Bucket=OUT_BUCKET, Prefix=expected_prefix)
        assert "Contents" in objs

        # Verify GIBS
        gibs_prefix = "L30/data/2020001"
        gibs_objs = s3_setup.list_objects(Bucket=GIBS_BUCKET, Prefix=gibs_prefix)
        assert "Contents" in gibs_objs
