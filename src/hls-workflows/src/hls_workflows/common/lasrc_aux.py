"""Resolve LaSRC ancillary (aux) file paths for the Rust lasrc port.

The Rust LaSRC (``lasrc``/``lasrc-rs`` package) takes each ancillary input as an
explicit path, unlike the C wrappers which discover them from ``LASRC_AUX_DIR``
internally. This module maps the existing ``LASRC_AUX_DIR`` layout to the seven
paths the Rust ``process_scene`` API (``lasrc.pipeline.AuxFilePaths``) expects.

The keys returned by :func:`resolve_lasrc_aux_paths` deliberately match the
``AuxFilePaths`` field names so a caller can do ``AuxFilePaths(**paths)``.

Expected ``LASRC_AUX_DIR`` layout (see reference docs / known-good
``data/lasrc_aux/``)::

    <aux>/
        CMGDEM.hdf
        ratiomapndwiexp.hdf
        LDCMLUT/      # Landsat LUTs (ANGLE_NEW.hdf, RES_LUT/TRANS_LUT/AERO_LUT ...)
        MSILUT/       # Sentinel-2 LUTs (same file names, different values)
        LADS/<year>/  # daily VIIRS/MODIS aerosol/water-vapor/ozone data

The two ``ANGLE_NEW.hdf`` files (one per LUT dir) are NOT interchangeable, so
Landsat resolves against ``LDCMLUT/`` and Sentinel-2 against ``MSILUT/``.
"""

from __future__ import annotations

import datetime as dt
import glob
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_LADS_AUX_SOURCES = ("VIIRS", "MODIS")


def _require(path: Path, description: str) -> Path:
    """Return ``path`` if it exists, else raise a descriptive error."""
    if not path.exists():
        raise FileNotFoundError(
            f"Could not find LaSRC aux {description}: {path}. "
            "Check LASRC_AUX_DIR and its LDCMLUT/MSILUT/LADS layout."
        )
    return path


def _glob_any(directory: Path, pattern: str, description: str) -> list[Path]:
    """Return all files in ``directory`` matching ``pattern``, raising if none.

    Matches are in filesystem listing order (``glob.glob``), not sorted.
    """
    matches = [Path(m) for m in glob.glob(str(directory / pattern))]
    if not matches:
        raise FileNotFoundError(
            f"Could not find LaSRC aux {description} matching '{pattern}' "
            f"in {directory}. Check LASRC_AUX_DIR and its layout."
        )
    return matches


def _glob_one(directory: Path, pattern: str, description: str) -> Path:
    """Return the single file in ``directory`` matching ``pattern``.

    Raises if zero or more than one match (ambiguous aux data).
    """
    matches = sorted(_glob_any(directory, pattern, description))
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous LaSRC aux {description}: multiple files match "
            f"'{pattern}' in {directory}: {[m.name for m in matches]}"
        )
    return matches[0]


def resolve_lasrc_aux_paths(
    *,
    is_sentinel: bool,
    acquisition: dt.datetime,
    aux_dir: Path | None = None,
    aux_source: str = "VIIRS",
) -> dict[str, Path]:
    """Resolve the seven Rust-LaSRC aux paths from ``LASRC_AUX_DIR``.

    Parameters
    ----------
    is_sentinel
        Resolve Sentinel-2 LUTs (``MSILUT/``) if True, else Landsat
        (``LDCMLUT/``).
    acquisition
        Scene acquisition datetime, used to select the daily LADS file.
    aux_dir
        Override the aux root. Defaults to the ``LASRC_AUX_DIR`` environment
        variable.
    aux_source
        Daily aerosol source: "VIIRS" (default) or "MODIS".

    Returns
    -------
    dict[str, Path]
        Validated paths keyed by ``AuxFilePaths`` field name (``angle_hdf``,
        ``intref_hdf``, ``transm_hdf``, ``sphera_hdf``, ``wv_oz_hdf``,
        ``dem_hdf``, ``ratio_hdf``). Every entry is guaranteed to exist on disk.
    """
    if aux_source not in _LADS_AUX_SOURCES:
        raise ValueError(
            f"Unknown aux_source {aux_source!r} (expected one of {_LADS_AUX_SOURCES})"
        )

    if aux_dir is None:
        env_aux = os.environ.get("LASRC_AUX_DIR")
        if not env_aux:
            raise RuntimeError(
                "LASRC_AUX_DIR is not set; cannot resolve LaSRC aux data paths."
            )
        aux_dir = Path(env_aux)

    aux_dir = _require(aux_dir, "root directory")

    lut_dir = _require(
        aux_dir / ("MSILUT" if is_sentinel else "LDCMLUT"),
        "LUT directory",
    )

    return {
        "angle_hdf": _require(lut_dir / "ANGLE_NEW.hdf", "angle LUT (ANGLE_NEW.hdf)"),
        "intref_hdf": _glob_one(lut_dir, "RES_LUT_*.hdf", "intrinsic reflectance LUT"),
        "transm_hdf": _glob_one(lut_dir, "TRANS_LUT_*.ASCII", "transmission LUT"),
        "sphera_hdf": _glob_one(lut_dir, "AERO_LUT_*.ASCII", "spherical albedo LUT"),
        "wv_oz_hdf": _resolve_lads_file(aux_dir, acquisition, aux_source),
        "dem_hdf": _require(aux_dir / "CMGDEM.hdf", "CMG DEM (CMGDEM.hdf)"),
        "ratio_hdf": _require(
            aux_dir / "ratiomapndwiexp.hdf", "band ratio / NDWI (ratiomapndwiexp.hdf)"
        ),
    }


def _resolve_lads_file(
    aux_dir: Path, acquisition: dt.datetime, aux_source: str
) -> Path:
    """Resolve the daily LADS water-vapor/ozone file for the scene date.

    VIIRS files are named like ``V*04ANC.A<year><doy>.*.h5``; MODIS files like
    ``M*<year><doy>*``. Both live under ``LADS/<year>/``.

    If several VIIRS files match, the first one listed by the filesystem is
    used, matching the upstream C wrappers (see :func:`_first_viirs_file`).
    """
    year = acquisition.strftime("%Y")
    doy = acquisition.strftime("%j")
    lads_year_dir = _require(aux_dir / "LADS" / year, f"LADS directory for {year}")
    description = f"{aux_source} daily water-vapor/ozone (DOY {doy})"

    if aux_source == "VIIRS":
        # e.g. VJ104ANC.A2026073.002.2026078091843.h5 / VNP04ANC.A2026073...
        pattern = f"V*04ANC.A{year}{doy}.*.h5"
        return _first_viirs_file(lads_year_dir, pattern, description)

    # MODIS, e.g. MOD04... / MYD04...
    return _glob_one(lads_year_dir, f"M*{year}{doy}*", description)


def _first_viirs_file(directory: Path, pattern: str, description: str) -> Path:
    """Return the first VIIRS LADS file matching ``pattern``, like upstream.

    This is not great! We should ideally use the latest processed version of
    the preferred product when we find more than one source.

    For now we keep to the same logic as "do_lasrc_{sentinel,landsat}.py" so
    we can verify the Rust port.
    """
    matches = _glob_any(directory, pattern, description)
    chosen = matches[0]
    if len(matches) > 1:
        logger.warning(
            "Multiple LaSRC aux %s files match '%s' in %s: %s. Using %s, the "
            "first in filesystem order, to match the upstream C LaSRC scripts.",
            description,
            pattern,
            directory,
            [m.name for m in matches],
            chosen.name,
        )
    return chosen
