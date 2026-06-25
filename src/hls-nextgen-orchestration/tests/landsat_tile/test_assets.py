"""Tests for Landsat tile EnvConfig S3 path construction."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from hls_nextgen_orchestration.landsat_tile.assets import EnvConfig

GRANULE_ID = "HLS.L30.T01TEST.2020001T000000.v2.0"
VI_GRANULE_ID = GRANULE_ID.replace("HLS.L30", "HLS-VI.L30")


@pytest.fixture
def config(tmp_path: Path) -> EnvConfig:
    return EnvConfig(
        job_id="test-job",
        pathrow_list=["025030"],
        date=dt.date(2020, 1, 1),  # %Y%j == 2020001
        mgrs="01TEST",
        mgrs_ulx="0",
        mgrs_uly="0",
        input_bucket="test-input-bucket",
        output_bucket="test-output-bucket",
        gibs_bucket="test-gibs-bucket",
        working_dir=tmp_path,
    )


def test_gibs_path_is_interpolated_uri(config: EnvConfig) -> None:
    """gibs_path.uri must interpolate the bucket and key.

    Regression: a missing ``f`` prefix produced the literal string
    "s3://{self.gibs_bucket}/{key}" instead of a real path.
    """
    path = config.gibs_path

    assert path.bucket == "test-gibs-bucket"
    assert path.key == "L30/data/2020001"
    assert path.uri == "s3://test-gibs-bucket/L30/data/2020001"
    assert "{" not in path.uri and "}" not in path.uri


def test_output_path_lives_in_output_bucket(config: EnvConfig) -> None:
    path = config.output_path(GRANULE_ID)

    assert path.bucket == "test-output-bucket"
    assert path.key == f"L30/data/2020001/{GRANULE_ID}"


def test_vi_path_lives_in_output_bucket(config: EnvConfig) -> None:
    path = config.vi_path(VI_GRANULE_ID)

    assert path.bucket == "test-output-bucket"
    assert path.key == f"L30_VI/data/2020001/{VI_GRANULE_ID}"


def test_path_keys_are_bare(config: EnvConfig) -> None:
    """No path key may carry a scheme or bucket (would corrupt upload keys)."""
    paths = [
        config.gibs_path,
        config.output_path(GRANULE_ID),
        config.vi_path(VI_GRANULE_ID),
    ]
    for path in paths:
        assert not path.key.startswith("s3://")
        assert path.bucket not in path.key
