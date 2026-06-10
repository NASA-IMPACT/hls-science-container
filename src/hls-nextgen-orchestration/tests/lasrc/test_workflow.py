from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from hls_nextgen_orchestration.lasrc import sentinel as s2_lasrc
from hls_nextgen_orchestration.lasrc.workflow import _is_sentinel, construct_pipeline

S2_GRANULE = "S2B_MSIL1C_20230602T155819_N0510_R097_T18SUJ_20240911T062140"
L_GRANULE = "LC09_L1TP_001066_20260314_20260314_02_T1"

# The rust path validates only the download command at construction time
# (RunLaSRCRust imports the lasrc Python API lazily inside run(), so it needs no
# command on PATH to build the pipeline).
_DOWNLOAD_COMMANDS = ["unzip", "download_landsat"]

# The C path instantiates the real ESPA/LaSRC tasks, which each validate their
# external command in __post_init__. Note: NO Fmask commands -- the just-LaSRC C
# path does not run Fmask.
_C_PATH_COMMANDS = _DOWNLOAD_COMMANDS + [
    "gdal_translate",
    "check_solar_zenith_sentinel",
    "check_solar_zenith_landsat",
    "apply_s2_quality_mask",
    "sentinel-derive-angle",
    "unpackage_s2.py",
    "convert_sentinel_to_espa",
    "do_lasrc_sentinel.py",
    "convert_lpgs_to_espa",
    "do_lasrc_landsat.py",
]


def _install(
    install_mock_binaries: Callable[[dict[str, str]], Path], names: list[str]
) -> Path:
    return install_mock_binaries({name: "#!/bin/bash\nexit 0\n" for name in names})


@pytest.fixture
def download_bins(
    install_mock_binaries: Callable[[dict[str, str]], Path],
) -> Path:
    """Minimal binaries for the rust path (just the download command)."""
    return _install(install_mock_binaries, _DOWNLOAD_COMMANDS)


@pytest.fixture
def c_path_bins(
    install_mock_binaries: Callable[[dict[str, str]], Path],
) -> Path:
    """Binaries the C path's ESPA/LaSRC tasks validate at construction."""
    return _install(install_mock_binaries, _C_PATH_COMMANDS)


@pytest.fixture
def base_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "lasrc-job")
    monkeypatch.setenv("INPUT_BUCKET", "in-bucket")
    monkeypatch.setenv("OUTPUT_BUCKET", "out-bucket")
    monkeypatch.setenv("GIBS_OUTPUT_BUCKET", "gibs-bucket")
    monkeypatch.setenv("PREFIX", "L30")
    monkeypatch.setenv("ACCODE", "LaSRC")
    monkeypatch.setenv("LASRC_AUX_DIR", str(tmp_path / "aux"))
    monkeypatch.setenv("SCRATCH_DIR", str(tmp_path / "scratch"))


def test_is_sentinel_detection() -> None:
    assert _is_sentinel(S2_GRANULE) is True
    assert _is_sentinel(L_GRANULE) is False
    with pytest.raises(ValueError, match="Could not identify"):
        _is_sentinel("not-a-granule")


# ----- Rust path (the new behaviour): minimal Download -> LaSRC -> Upload


def test_sentinel_rust_pipeline_shape(download_bins: Path, base_env: None) -> None:
    pipeline = construct_pipeline(S2_GRANULE, lasrc_version="rust")
    classes = [type(t).__name__ for t in pipeline.execution_order]
    assert any(t.startswith("RunLaSRCRust") for t in classes)
    assert any(t.startswith("UploadLaSRCDebug") for t in classes)
    # No ESPA-conversion chain on the rust path.
    assert not any(t.startswith("PrepareEspa") for t in classes)
    assert not any(t.startswith("RunLaSRC-") for t in classes)


def test_rust_landsat_is_minimal(download_bins: Path, base_env: None) -> None:
    """The Landsat rust path is exactly Download -> LaSRC -> Upload (+ EnvSource)."""
    pipeline = construct_pipeline(L_GRANULE, lasrc_version="rust")
    classes = [type(t).__name__ for t in pipeline.execution_order]
    assert classes == [
        "EnvSource",
        "DownloadGranule",
        "RunLaSRCRust",
        "UploadLaSRCDebug",
    ]


def test_local_granule_uses_local_tasks(
    download_bins: Path, base_env: None, tmp_path: Path
) -> None:
    granule_dir = tmp_path / "local_granule"
    granule_dir.mkdir()
    pipeline = construct_pipeline(
        L_GRANULE, lasrc_version="rust", local_granule=granule_dir
    )
    classes = [type(t).__name__ for t in pipeline.execution_order]
    assert "LocalGranule" in classes
    assert "DownloadGranule" not in classes


def test_sentinel_rust_provides_aerosol_qa(download_bins: Path, base_env: None) -> None:
    """Rust S2 task reuses the aerosol QA asset so upload ordering matches C."""
    pipeline = construct_pipeline(S2_GRANULE, lasrc_version="rust")
    rust = next(
        t for t in pipeline.execution_order if isinstance(t, s2_lasrc.RunLaSRCRust)
    )
    provided = {a.key for a in rust.provides}
    assert any("lasrc_aerosol_qa" in key for key in provided)


# ----- C path: reuses the existing ESPA chain up through LaSRC, sans Fmask


def test_sentinel_c_pipeline_shape(c_path_bins: Path, base_env: None) -> None:
    pipeline = construct_pipeline(S2_GRANULE, lasrc_version="c")
    classes = [type(t).__name__ for t in pipeline.execution_order]
    assert any(t.startswith("RunLaSRC-") for t in classes)
    assert any(t.startswith("PrepareEspaInputNoFmask") for t in classes)
    assert not any(t.startswith("RunLaSRCRust") for t in classes)
    # The just-LaSRC C path does not run Fmask.
    assert not any(t.startswith("RunFmask") for t in classes)


def test_landsat_c_pipeline_shape(c_path_bins: Path, base_env: None) -> None:
    pipeline = construct_pipeline(L_GRANULE, lasrc_version="c")
    classes = [type(t).__name__ for t in pipeline.execution_order]
    assert "RunLaSRC" in classes
    assert "ConvertToEspa" in classes
    assert "ScanlineNoFmask" in classes
    assert "RunLaSRCRust" not in classes
    # The just-LaSRC C path does not run Fmask.
    assert not any(t.startswith("RunFmask") for t in classes)
