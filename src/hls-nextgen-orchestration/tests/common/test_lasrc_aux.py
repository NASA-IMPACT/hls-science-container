from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from hls_nextgen_orchestration.common.lasrc_aux import resolve_lasrc_aux_paths


def _make_aux_tree(root: Path) -> None:
    """Create a minimal but valid LASRC_AUX_DIR layout under ``root``."""
    (root / "CMGDEM.hdf").touch()
    (root / "ratiomapndwiexp.hdf").touch()

    for lut in ("LDCMLUT", "MSILUT"):
        lut_dir = root / lut
        lut_dir.mkdir()
        version = "V2.0" if lut == "LDCMLUT" else "V3.0"
        # RES/angle LUTs are HDF4; TRANS/AERO LUTs are ASCII (matches upstream).
        (lut_dir / "ANGLE_NEW.hdf").touch()
        (lut_dir / f"RES_LUT_{version}-X.hdf").touch()
        (lut_dir / f"TRANS_LUT_{version}-X.ASCII").touch()
        (lut_dir / f"AERO_LUT_{version}-X.ASCII").touch()

    # Daily LADS file for 2026 DOY 073 (2026-03-14), VIIRS naming.
    lads = root / "LADS" / "2026"
    lads.mkdir(parents=True)
    (lads / "VJ104ANC.A2026073.001.h5").touch()
    (lads / "MOD04_2026073_global.hdf").touch()


@pytest.fixture
def aux_dir(tmp_path: Path) -> Path:
    root = tmp_path / "lasrc_aux"
    root.mkdir()
    _make_aux_tree(root)
    return root


ACQ = dt.datetime(2026, 3, 14)


# Keys must stay in sync with lasrc.pipeline.AuxFilePaths field names so callers
# can do AuxFilePaths(**resolve_lasrc_aux_paths(...)).
EXPECTED_KEYS = {
    "angle_hdf",
    "intref_hdf",
    "transm_hdf",
    "sphera_hdf",
    "wv_oz_hdf",
    "dem_hdf",
    "ratio_hdf",
}


def test_resolve_returns_auxfilepaths_keys(aux_dir: Path) -> None:
    paths = resolve_lasrc_aux_paths(is_sentinel=False, acquisition=ACQ, aux_dir=aux_dir)
    assert set(paths) == EXPECTED_KEYS


def test_resolve_landsat_uses_ldcmlut(aux_dir: Path) -> None:
    paths = resolve_lasrc_aux_paths(is_sentinel=False, acquisition=ACQ, aux_dir=aux_dir)
    assert paths["angle_hdf"] == aux_dir / "LDCMLUT" / "ANGLE_NEW.hdf"
    assert paths["intref_hdf"].parent.name == "LDCMLUT"
    assert paths["intref_hdf"].name.startswith("RES_LUT_")
    assert paths["transm_hdf"].name.startswith("TRANS_LUT_")
    assert paths["sphera_hdf"].name.startswith("AERO_LUT_")
    assert paths["dem_hdf"] == aux_dir / "CMGDEM.hdf"
    assert paths["ratio_hdf"] == aux_dir / "ratiomapndwiexp.hdf"


def test_resolve_sentinel_uses_msilut(aux_dir: Path) -> None:
    paths = resolve_lasrc_aux_paths(is_sentinel=True, acquisition=ACQ, aux_dir=aux_dir)
    assert paths["angle_hdf"] == aux_dir / "MSILUT" / "ANGLE_NEW.hdf"
    assert paths["intref_hdf"].parent.name == "MSILUT"


def test_resolve_viirs_lads_file_for_date(aux_dir: Path) -> None:
    paths = resolve_lasrc_aux_paths(
        is_sentinel=False, acquisition=ACQ, aux_dir=aux_dir, aux_source="VIIRS"
    )
    assert paths["wv_oz_hdf"] == aux_dir / "LADS" / "2026" / "VJ104ANC.A2026073.001.h5"


def test_resolve_modis_lads_file_for_date(aux_dir: Path) -> None:
    paths = resolve_lasrc_aux_paths(
        is_sentinel=False, acquisition=ACQ, aux_dir=aux_dir, aux_source="MODIS"
    )
    assert paths["wv_oz_hdf"] == aux_dir / "LADS" / "2026" / "MOD04_2026073_global.hdf"


def test_missing_lads_file_raises(aux_dir: Path) -> None:
    # Year dir (2026) exists, but there is no file for this day-of-year.
    missing_day = dt.datetime(2026, 12, 31)
    with pytest.raises(FileNotFoundError, match="water-vapor/ozone"):
        resolve_lasrc_aux_paths(
            is_sentinel=False, acquisition=missing_day, aux_dir=aux_dir
        )


def test_missing_lut_raises(tmp_path: Path) -> None:
    root = tmp_path / "empty_aux"
    root.mkdir()
    with pytest.raises(FileNotFoundError, match="LUT directory"):
        resolve_lasrc_aux_paths(is_sentinel=False, acquisition=ACQ, aux_dir=root)


def test_unset_aux_dir_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LASRC_AUX_DIR", raising=False)
    with pytest.raises(RuntimeError, match="LASRC_AUX_DIR is not set"):
        resolve_lasrc_aux_paths(is_sentinel=False, acquisition=ACQ)


def test_unknown_aux_source_raises(aux_dir: Path) -> None:
    with pytest.raises(ValueError, match="Unknown aux_source"):
        resolve_lasrc_aux_paths(
            is_sentinel=False, acquisition=ACQ, aux_dir=aux_dir, aux_source="BOGUS"
        )
