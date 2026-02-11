from __future__ import annotations

import os
import stat
from collections.abc import Callable, Generator
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_s3 import S3Client


@pytest.fixture
def mock_aws_s3() -> Generator[None, None, None]:
    """
    Mock AWS S3 services using Moto.
    """
    with mock_aws():
        yield


@pytest.fixture
def s3_client(mock_aws_s3: None) -> S3Client:
    """
    Returns a mocked S3 client.
    """
    return boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def install_mock_binaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[dict[str, str]], Path]:
    """
    Factory fixture to create dummy executable scripts on the PATH.
    """

    def _install(scripts: dict[str, str]) -> Path:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir(exist_ok=True)

        for name, content in scripts.items():
            path = bin_dir / name
            path.write_text(content, encoding="utf-8")
            # Make executable (chmod +x)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)

        # Prepend to PATH
        monkeypatch.setenv("PATH", f"{str(bin_dir)}:{os.environ['PATH']}")
        return bin_dir

    return _install
