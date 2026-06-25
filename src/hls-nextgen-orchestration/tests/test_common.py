"""Tests for shared data containers in hls_nextgen_orchestration.common."""

from __future__ import annotations

from hls_nextgen_orchestration.common import S3Path


def test_s3path_uri_interpolates_bucket_and_key() -> None:
    """uri must be a real s3:// URI, not a literal f-string template.

    Regression: a missing ``f`` prefix once produced the literal string
    "s3://{self.gibs_bucket}/{key}" instead of an interpolated path.
    """
    path = S3Path(bucket="my-bucket", key="S30/data/2020001")

    assert path.uri == "s3://my-bucket/S30/data/2020001"
    assert "{" not in path.uri and "}" not in path.uri


def test_s3path_key_has_no_scheme_or_bucket() -> None:
    """key is bare (for boto3 APIs that take bucket and key separately).

    Regression: passing a full s3:// URI as the boto3 ``Key`` argument silently
    uploads to a key like "s3://bucket/foo.tif".
    """
    path = S3Path(bucket="my-bucket", key="S30/data/2020001")

    assert not path.key.startswith("s3://")
    assert "my-bucket" not in path.key


def test_s3path_truediv_appends_to_key_only() -> None:
    """The / operator extends the key and preserves the bucket."""
    path = S3Path(bucket="my-bucket", key="S30/data/2020001")

    child = path / "GIBS_ID_1"
    assert child.bucket == "my-bucket"
    assert child.key == "S30/data/2020001/GIBS_ID_1"
    assert child.uri == "s3://my-bucket/S30/data/2020001/GIBS_ID_1"

    # Chaining keeps building the key, never the bucket
    leaf = child / "tile.tif"
    assert leaf.key == "S30/data/2020001/GIBS_ID_1/tile.tif"


def test_s3path_truediv_normalizes_slashes() -> None:
    """Joining tolerates stray leading/trailing slashes without doubling them."""
    path = S3Path(bucket="b", key="prefix/")

    assert (path / "/child/").key == "prefix/child"


def test_s3path_str_is_uri() -> None:
    """str(S3Path) yields the full URI so it is safe in log messages."""
    path = S3Path(bucket="b", key="k")

    assert str(path) == "s3://b/k" == path.uri
