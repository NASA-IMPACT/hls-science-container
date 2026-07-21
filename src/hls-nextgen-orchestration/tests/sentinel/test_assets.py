"""Tests for Sentinel-2 EnvConfig S3 path construction."""

from __future__ import annotations

from pathlib import Path

from hls_nextgen_orchestration.sentinel.assets import EnvConfig

# Granule acquired 2020-01-01 -> %Y%j == 2020001
SENTINEL_PATH_TESTS = [
    "output_path",
    "vi_path",
    "gibs_path",
]


def test_gibs_path_is_interpolated_uri(sentinel_config: EnvConfig) -> None:
    """gibs_path.uri must interpolate the bucket and key.

    Regression: a missing ``f`` prefix produced the literal string
    "s3://{self.gibs_bucket}/{key}" instead of a real path.
    """
    path = sentinel_config.gibs_path

    assert path.bucket == "test-gibs-bucket"
    assert path.key == "S30/data/2020001"
    assert path.uri == "s3://test-gibs-bucket/S30/data/2020001"


def test_output_path_lives_in_output_bucket(sentinel_config: EnvConfig) -> None:
    path = sentinel_config.output_path

    assert path.bucket == "test-output-bucket"
    assert path.key.startswith("S30/data/2020001/HLS.S30.")
    assert path.uri == f"s3://test-output-bucket/{path.key}"


def test_vi_path_lives_in_output_bucket(sentinel_config: EnvConfig) -> None:
    path = sentinel_config.vi_path

    assert path.bucket == "test-output-bucket"
    assert path.key.startswith("S30_VI/data/2020001/HLS-VI.S30.")


def test_path_keys_are_bare(sentinel_config: EnvConfig) -> None:
    """No path key may carry a scheme or bucket (would corrupt upload keys)."""
    for attr in SENTINEL_PATH_TESTS:
        path = getattr(sentinel_config, attr)
        assert not path.key.startswith("s3://"), attr
        assert "{" not in path.uri and "}" not in path.uri, attr


def test_twin_granules_get_suffix() -> None:
    """Twin-granule runs append a 'twin' suffix to avoid clobbering."""
    config = EnvConfig(
        job_id="test-job",
        input_bucket="in",
        output_bucket="out",
        gibs_bucket="gibs",
        granule_ids=[
            "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841",
            "S2B_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841",
        ],
        working_dir=Path("/tmp"),
        ac_code="LaSRC v3.5.1.8",
        cloud_masking_code="Fmask v5.0.1",
    )

    assert config.output_path.key.endswith("/twin")
    assert config.vi_path.key.endswith("/twin")
    # GIBS path is not granule-specific, so it gets no twin suffix
    assert not config.gibs_path.key.endswith("/twin")
