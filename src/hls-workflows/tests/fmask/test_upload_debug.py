from __future__ import annotations

from pathlib import Path

from mypy_boto3_s3 import S3Client

from hls_workflows.fmask import landsat as l_fmask
from hls_workflows.fmask import sentinel as s2_fmask
from hls_workflows.landsat_ac.assets import CONFIG as L_CONFIG
from hls_workflows.landsat_ac.assets import FMASK_BIN
from hls_workflows.landsat_ac.assets import EnvConfig as LandsatEnvConfig
from hls_workflows.sentinel.assets import CONFIG as S2_CONFIG
from hls_workflows.sentinel.assets import EnvConfig as SentinelEnvConfig
from hls_workflows.sentinel.assets import fmask_bin_asset

DEBUG_BUCKET = "test-debug-bucket"
S2_GRANULE = "S2B_MSIL1C_20250116T141709_N0511_R010_T19HDS_20250116T205310"
L_GRANULE = "LC09_L1TP_001066_20260314_20260314_02_T1"
NON_PRODUCTS = ("fmask.bin", "fmask.hdr", "B01.tif", "not_a_product.txt")


def _keys(s3_client: S3Client, bucket: str) -> list[str]:
    response = s3_client.list_objects_v2(Bucket=bucket)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


def _touch(directory: Path, *names: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).touch()


def test_sentinel_upload_only_mask_flattened(
    s3_client: S3Client, tmp_path: Path
) -> None:
    config = SentinelEnvConfig(
        job_id="test-job",
        granule_ids=[S2_GRANULE],
        input_bucket="test-input-bucket",
        output_bucket="test-output-bucket",
        gibs_bucket="test-gibs-bucket",
        working_dir=tmp_path / "working",
        ac_code="LaSRC v3.5.1.8",
        cloud_masking_code="Fmask v5.0.1",
        debug_bucket=DEBUG_BUCKET,
    )
    s3_client.create_bucket(Bucket=DEBUG_BUCKET)

    granule_dir = config.working_dir / S2_GRANULE
    _touch(granule_dir / f"{S2_GRANULE}.SAFE", f"{S2_GRANULE}_UPL.tif", *NON_PRODUCTS)
    _touch(config.working_dir / "SOME_OTHER_GRANULE", "OTHER_UPL.tif")

    task = s2_fmask.UploadFmaskDebug.map(S2_GRANULE)(name="upload", prefix="fmask")
    task.run({S2_CONFIG: config, fmask_bin_asset(S2_GRANULE): Path("unused")})

    assert _keys(s3_client, DEBUG_BUCKET) == [
        f"fmask/{S2_GRANULE}/{S2_GRANULE}_UPL.tif"
    ]


def test_landsat_upload_only_mask_flattened(
    s3_client: S3Client, tmp_path: Path
) -> None:
    config = LandsatEnvConfig(
        job_id="test-job",
        granule=L_GRANULE,
        input_bucket="test-input-bucket",
        output_bucket="test-output-bucket",
        ac_code="LaSRC",
        cloud_masking_code="Fmask v5.0.1",
        working_dir=tmp_path / "job",
        granule_dir=tmp_path / "job" / L_GRANULE,
        debug_bucket=DEBUG_BUCKET,
    )
    s3_client.create_bucket(Bucket=DEBUG_BUCKET)
    _touch(config.granule_dir, f"{L_GRANULE}_UPL.tif", *NON_PRODUCTS)

    task = l_fmask.UploadFmaskDebug("upload", prefix="fmask")
    task.run({L_CONFIG: config, FMASK_BIN: Path("unused")})

    assert _keys(s3_client, DEBUG_BUCKET) == [f"fmask/{L_GRANULE}/{L_GRANULE}_UPL.tif"]


def test_upload_noop_without_debug_bucket(s3_client: S3Client, tmp_path: Path) -> None:
    config = LandsatEnvConfig(
        job_id="test-job",
        granule=L_GRANULE,
        input_bucket="test-input-bucket",
        output_bucket="test-output-bucket",
        ac_code="LaSRC",
        cloud_masking_code="Fmask v5.0.1",
        working_dir=tmp_path / "job",
        granule_dir=tmp_path / "job" / L_GRANULE,
        debug_bucket=None,
    )
    _touch(config.granule_dir, f"{L_GRANULE}_UPL.tif")

    task = l_fmask.UploadFmaskDebug("upload", prefix="fmask")
    task.run({L_CONFIG: config, FMASK_BIN: Path("unused")})
