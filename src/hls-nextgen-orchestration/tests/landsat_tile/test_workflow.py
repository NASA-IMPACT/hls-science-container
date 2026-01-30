from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import PropertyMock, patch

import pytest

from hls_nextgen_orchestration.landsat_tile.assets import UPLOAD_COMPLETE
from hls_nextgen_orchestration.landsat_tile.workflow import construct_pipeline

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

# --- Configuration Constants ---
JOB_ID = "job-tile-1"
PATHROW_LIST = "025030"
DATE_STR = "2020-01-01"
MGRS = "12ABC"
MGRS_ULX = "300000"
MGRS_ULY = "4000000"
IN_BUCKET = "input-bucket"
OUT_BUCKET = "output-bucket"
GIBS_BUCKET = "gibs-bucket"
# Mocked time from the binary
SCENE_TIME = "101010"


@dataclass
class TileTestContext:
    job_id: str
    pathrow_list: str
    date: str
    mgrs: str
    mgrs_ulx: str
    mgrs_uly: str
    in_bucket: str
    out_bucket: str
    gibs_bucket: str
    scene_time: str

    @property
    def full_granule_id(self) -> str:
        # HLS.L30.T{MGRS}.{Year}{DOY}T{HMS}.v2.0
        # 2020-01-01 is DOY 001
        return f"HLS.L30.T{self.mgrs}.2020001T{self.scene_time}.v2.0"

    @property
    def expected_s3_prefix(self) -> str:
        # L30/data/{Year}{DOY}/{GranuleID}
        return f"L30/data/2020001/{self.full_granule_id}"


@pytest.fixture
def tile_context(s3_client: S3Client, tmp_path, monkeypatch) -> TileTestContext:
    """
    Prepares the S3 environment, sets environment variables, and returns
    config values for assertions.
    """
    # 1. Setup S3
    s3_client.create_bucket(Bucket=IN_BUCKET)
    s3_client.create_bucket(Bucket=OUT_BUCKET)
    s3_client.create_bucket(Bucket=GIBS_BUCKET)

    # Upload dummy input: {year}-{month}-{day}/{pathrow}/{date}_{pathrow}.hdf
    input_key = f"2020-01-01/{PATHROW_LIST}/2020-01-01_{PATHROW_LIST}.hdf"
    s3_client.put_object(Bucket=IN_BUCKET, Key=input_key, Body="dummy content")

    # 2. Setup Environment Variables
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("PATHROW_LIST", PATHROW_LIST)
    monkeypatch.setenv("DATE", DATE_STR)
    monkeypatch.setenv("MGRS", MGRS)
    monkeypatch.setenv("MGRS_ULX", MGRS_ULX)  # Added missing var
    monkeypatch.setenv("MGRS_ULY", MGRS_ULY)  # Added missing var
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", GIBS_BUCKET)
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))

    return TileTestContext(
        job_id=JOB_ID,
        pathrow_list=PATHROW_LIST,
        date=DATE_STR,
        mgrs=MGRS,
        mgrs_ulx=MGRS_ULX,
        mgrs_uly=MGRS_ULY,
        in_bucket=IN_BUCKET,
        out_bucket=OUT_BUCKET,
        gibs_bucket=GIBS_BUCKET,
        scene_time=SCENE_TIME,
    )


def test_pipeline_end_to_end(
    mock_binaries, tile_context: TileTestContext, s3_client: S3Client
):
    """
    Runs the full landsat tile pipeline using mocked binaries and mocked S3.
    """
    # 1. Construct Pipeline
    pipeline = construct_pipeline()

    # 2. Run Pipeline
    context = pipeline.run()

    # 3. Verify Success
    assert context.exit_code == 0
    assert context.get(UPLOAD_COMPLETE) is True

    # 4. Verify Side Effects (S3 Upload)
    # Check Main Product
    objs = s3_client.list_objects(
        Bucket=tile_context.out_bucket, Prefix=tile_context.expected_s3_prefix
    )
    assert "Contents" in objs
    keys = [o["Key"] for o in objs["Contents"]]

    # Verify specific expected files exist
    assert any(f"{tile_context.full_granule_id}.jpg" in k for k in keys)
    assert any(f"{tile_context.full_granule_id}.json" in k for k in keys)

    # Verify GIBS Uploads
    gibs_prefix = "L30/data/2020001"
    gibs_objs = s3_client.list_objects(
        Bucket=tile_context.gibs_bucket, Prefix=gibs_prefix
    )
    assert "Contents" in gibs_objs
