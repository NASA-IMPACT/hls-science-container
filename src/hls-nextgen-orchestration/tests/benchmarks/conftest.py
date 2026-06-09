from __future__ import annotations

import os
from pathlib import Path

import boto3
import pytest

_AUX_BUCKET = os.environ.get("BENCHMARK_AUX_BUCKET", "")
_AUX_PREFIX = os.environ.get("BENCHMARK_AUX_PREFIX", "")

_S2_GRANULE_IDS = [
    g for g in os.environ.get("BENCHMARK_S2_GRANULE_IDS", "").split(",") if g
]
_LS_GRANULE_IDS = [
    g for g in os.environ.get("BENCHMARK_LS_GRANULE_IDS", "").split(",") if g
]
_INPUT_BUCKET = os.environ.get("BENCHMARK_INPUT_BUCKET", "")
_INPUT_PREFIX = os.environ.get("BENCHMARK_INPUT_PREFIX", "")


def _s3_sync(bucket: str, prefix: str, local_dir: Path) -> None:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) :].lstrip("/")
            if not rel:
                continue
            dest = local_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(dest))


@pytest.fixture(scope="session", autouse=True)
def lasrc_aux_dir(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Ensure LASRC_AUX_DIR is set before any benchmark runs.

    If LASRC_AUX_DIR is already set (e.g. scripts/shell mounts data/lasrc_aux/
    into the container), the existing value is used as-is.  Otherwise the aux
    data is synced from S3 using BENCHMARK_AUX_BUCKET + BENCHMARK_AUX_PREFIX
    and the env var is pointed at the downloaded copy.
    """
    if os.environ.get("LASRC_AUX_DIR"):
        return
    if not _AUX_BUCKET or not _AUX_PREFIX:
        pytest.skip(
            "LASRC_AUX_DIR not set and BENCHMARK_AUX_BUCKET/AUX_PREFIX not configured"
        )
    aux_dir = tmp_path_factory.mktemp("lasrc_aux")
    _s3_sync(_AUX_BUCKET, _AUX_PREFIX, aux_dir)
    os.environ["LASRC_AUX_DIR"] = str(aux_dir)


@pytest.fixture(scope="session", autouse=True)
def benchmark_env() -> None:
    """Set env vars required by workflow EnvSource nodes.

    Pipelines read OUTPUT_BUCKET, ACCODE, etc. from os.environ at runtime
    even when using local input data (upload=False). Set non-sensitive
    placeholders so the workflows initialize cleanly.

    GRANULE_LIST is intentionally not set here — each Sentinel benchmark
    test sets it to its own granule_id before calling pipeline.run().
    """
    defaults: dict[str, str] = {
        "ACCODE": "LaSRC v3.5.1.0",
        "INPUT_BUCKET": _INPUT_BUCKET,
        "OUTPUT_BUCKET": "benchmark-output-unused",
        "GIBS_OUTPUT_BUCKET": "benchmark-gibs-unused",
        "PREFIX": "L30",
    }
    for key, value in defaults.items():
        if value and key not in os.environ:
            os.environ[key] = value


@pytest.fixture(scope="session")
def s2_local_zips(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """Download all Sentinel-2 benchmark granule zips from S3 once per session.

    Returns a mapping of granule_id -> local zip path.
    Expected S3 key: ``{BENCHMARK_INPUT_PREFIX}/{granule_id}.zip``.
    """
    if not _S2_GRANULE_IDS or not _INPUT_BUCKET or not _INPUT_PREFIX:
        return {}

    session_dir = tmp_path_factory.mktemp("s2_data")
    s3 = boto3.client("s3")
    result: dict[str, Path] = {}
    for granule_id in _S2_GRANULE_IDS:
        zip_path = session_dir / f"{granule_id}.zip"
        s3.download_file(
            _INPUT_BUCKET, f"{_INPUT_PREFIX}/{granule_id}.zip", str(zip_path)
        )
        result[granule_id] = zip_path
    return result


@pytest.fixture(scope="session")
def ls_local_dirs(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """Download all Landsat benchmark granule directories from S3 once per session.

    Returns a mapping of granule_id -> local directory path.
    Uses ``aws s3 sync``. Expected S3 path per granule:
    ``s3://{BENCHMARK_INPUT_BUCKET}/{BENCHMARK_INPUT_PREFIX}/{granule_id}/``.
    """
    if not _LS_GRANULE_IDS or not _INPUT_BUCKET or not _INPUT_PREFIX:
        return {}

    session_dir = tmp_path_factory.mktemp("ls_data")
    result: dict[str, Path] = {}
    for granule_id in _LS_GRANULE_IDS:
        granule_dir = session_dir / granule_id
        granule_dir.mkdir()
        _s3_sync(_INPUT_BUCKET, f"{_INPUT_PREFIX}/{granule_id}/", granule_dir)
        result[granule_id] = granule_dir
    return result
