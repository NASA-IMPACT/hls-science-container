from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from hls_workflows.landsat_ac.assets import UPLOAD_COMPLETE
from hls_workflows.landsat_ac.tasks import RunFmaskV5
from hls_workflows.landsat_ac.workflow import construct_pipeline
from hls_workflows.pipeline import Pipeline

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

JOB_ID = "workflow-job-2"
GRANULE = "LC08_L1TP_025030_20200101_20200114_01_T1"
IN_BUCKET = "usgs-landsat"
OUT_BUCKET = "processed-data"


@pytest.fixture
def s3_setup(s3_client: S3Client) -> S3Client:
    s3_client.create_bucket(Bucket=IN_BUCKET)
    s3_client.create_bucket(Bucket=OUT_BUCKET)
    return s3_client


def test_landsat_pipeline_fmask(
    mock_binaries: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify the Landsat pipeline uses the Fmask v5 task."""
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))

    pipeline = construct_pipeline(granule_id=GRANULE)
    fmask_tasks = [t for t in pipeline.execution_order if isinstance(t, RunFmaskV5)]

    assert len(fmask_tasks) == 1


def test_landsat_pipeline_end_to_end(
    mock_binaries: Path,
    s3_setup: S3Client,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))

    pipeline = construct_pipeline(granule_id=GRANULE)
    context = pipeline.run()

    assert context.get(UPLOAD_COMPLETE) is True


def test_cleanup_defaults_on(mock_binaries: Path) -> None:
    assert construct_pipeline(granule_id=GRANULE).cleanup_working_dir is True


def test_cleanup_can_be_disabled(mock_binaries: Path) -> None:
    pipeline = construct_pipeline(granule_id=GRANULE, cleanup_working_dir=False)

    assert pipeline.cleanup_working_dir is False


C_ORDER = [
    "EnvSource",
    "DownloadGranule",
    "CheckSolarZenith",
    "RunFmaskV5",
    "ConvertScanline",
    "ConvertToEspa",
    "RunLaSRC",
    "ParseMetadata",
    "RenameAngleBands",
    "CreateHlsXml",
    "ConvertToHdf",
    "AddFmaskSds",
    "UploadResults",
]


@pytest.fixture
def ac_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("INPUT_BUCKET", IN_BUCKET)
    monkeypatch.setenv("OUTPUT_BUCKET", OUT_BUCKET)
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path))


def _names(pipeline: Pipeline) -> list[str]:
    return [type(t).__name__ for t in pipeline.execution_order]


def test_c_path_unchanged(mock_binaries: Path, ac_env: None) -> None:
    assert _names(construct_pipeline(granule_id=GRANULE)) == C_ORDER


def test_rust_path_swaps_only_lasrc(mock_binaries: Path, ac_env: None) -> None:
    order = _names(construct_pipeline(granule_id=GRANULE, lasrc_version="rust"))

    i = C_ORDER.index("RunLaSRC")
    assert order == C_ORDER[:i] + ["RunLaSRCRust", "RegisterLaSRC"] + C_ORDER[i + 1 :]


def test_metrics_prop_lasrc_version(mock_binaries: Path, ac_env: None) -> None:
    pipeline = construct_pipeline(granule_id=GRANULE, lasrc_version="rust")

    assert pipeline.metrics is not None
    assert pipeline.metrics.pipeline_props["lasrc_version"] == "rust"
    assert "lasrc_version" not in pipeline.metrics.pipeline_dims
