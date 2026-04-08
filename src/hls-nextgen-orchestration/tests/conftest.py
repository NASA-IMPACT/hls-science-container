from __future__ import annotations

import os
import stat
from collections.abc import Callable, Generator
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_logs import CloudWatchLogsClient
from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.metrics import MetricsCollector


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def mocked_aws(aws_credentials: None) -> Generator[None, None, None]:
    """
    Mock AWS S3 services using Moto.
    """
    with mock_aws():
        yield


@pytest.fixture
def s3_client(mocked_aws: None) -> S3Client:
    """
    Returns a mocked S3 client.
    """
    return boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def logs_client(mocked_aws: None) -> CloudWatchLogsClient:
    """
    Returns a mocked Cloudwatch client.
    """
    return boto3.client("logs", region_name="us-east-1")


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


@pytest.fixture
def log_group() -> str:
    return "/hls/nextgen/test"


@pytest.fixture
def log_stream() -> str:
    return "test-job"


@pytest.fixture
def metrics_env(
    logs_client: CloudWatchLogsClient,
    monkeypatch: pytest.MonkeyPatch,
    log_group: str,
    log_stream: str,
) -> CloudWatchLogsClient:
    logs_client.create_log_group(logGroupName=log_group)
    logs_client.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
    monkeypatch.setenv("METRIC_LOG_GROUP_NAME", log_group)
    monkeypatch.setenv("AWS_BATCH_JOB_ID", log_stream)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    return logs_client


@pytest.fixture
def metric_collector(metrics_env: CloudWatchLogsClient) -> MetricsCollector:
    """Enabled and configured MetricsCollector"""
    return MetricsCollector(client=metrics_env)
