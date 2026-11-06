from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from hls_workflows.fmask import sentinel as s2_fmask
from hls_workflows.fmask.workflow import construct_pipeline

S2_GRANULE = "S2B_MSIL1C_20230602T155819_N0510_R097_T18SUJ_20240911T062140"
L_GRANULE = "LC09_L1TP_001066_20260314_20260314_02_T1"


@pytest.fixture
def fmask_bins(install_mock_binaries: Callable[[dict[str, str]], Path]) -> Path:
    """Binaries the download and Fmask tasks validate at construction."""
    return install_mock_binaries(
        {name: "#!/bin/bash\nexit 0\n" for name in ("unzip", "fmask", "gdal_translate")}
    )


@pytest.fixture
def base_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "fmask-job")
    monkeypatch.setenv("INPUT_BUCKET", "in-bucket")
    monkeypatch.setenv("OUTPUT_BUCKET", "out-bucket")
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", "gibs-bucket")
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path / "scratch"))


def _classes(granule_id: str, **kwargs: object) -> list[str]:
    pipeline = construct_pipeline(granule_id, **kwargs)  # type: ignore[arg-type]
    return [type(t).__name__ for t in pipeline.execution_order]


def test_sentinel_pipeline_shape(fmask_bins: Path, base_env: None) -> None:
    assert _classes(S2_GRANULE) == [
        "EnvSource",
        f"DownloadSentinelGranule-{S2_GRANULE}",
        f"GetGranuleDir-{S2_GRANULE}",
        f"ApplyQualityMaskNoSolar-{S2_GRANULE}",
        f"RunFmaskV5-{S2_GRANULE}",
        f"UploadFmaskDebug-{S2_GRANULE}",
    ]


def test_landsat_pipeline_shape(fmask_bins: Path, base_env: None) -> None:
    assert _classes(L_GRANULE) == [
        "EnvSource",
        "DownloadGranule",
        "RunFmaskV5",
        "UploadFmaskDebug",
    ]


def test_sentinel_mask_does_not_require_solar(fmask_bins: Path, base_env: None) -> None:
    pipeline = construct_pipeline(S2_GRANULE)
    mask = next(
        t
        for t in pipeline.execution_order
        if isinstance(t, s2_fmask.ApplyQualityMaskNoSolar)
    )
    assert not any("solar" in a.key for a in mask.requires)


@pytest.mark.parametrize(
    ("granule_id", "match"),
    [
        ("HLS.S30.T18TYL.2020001T153621.v2.0", "got HlsGranule"),
        ("not-a-granule", "Could not parse"),
    ],
)
def test_rejects_non_l1_granule_ids(
    base_env: None, granule_id: str, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        construct_pipeline(granule_id)


@pytest.mark.parametrize("granule_id", [S2_GRANULE, L_GRANULE])
def test_local_granule_uses_local_tasks(
    fmask_bins: Path, base_env: None, tmp_path: Path, granule_id: str
) -> None:
    local = tmp_path / "local_granule"
    local.mkdir()
    classes = _classes(granule_id, local_granule=local)
    assert any(c.startswith(("LocalGranule", "LocalSentinelGranule")) for c in classes)
    assert not any(
        c.startswith(("DownloadGranule", "DownloadSentinel")) for c in classes
    )
