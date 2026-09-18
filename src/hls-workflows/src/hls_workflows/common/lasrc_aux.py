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
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_LADS_AUX_SOURCES = ("VIIRS", "MODIS")
_VIIRS_PROCESSED_AT_FORMAT = "%Y%j%H%M%S"


def _require(path: Path, description: str) -> Path:
    """Return ``path`` if it exists, else raise a descriptive error."""
    if not path.exists():
        raise FileNotFoundError(
            f"Could not find LaSRC aux {description}: {path}. "
            "Check LASRC_AUX_DIR and its LDCMLUT/MSILUT/LADS layout."
        )
    return path


def _glob_any(directory: Path, pattern: str, description: str) -> list[Path]:
    """Return all files in ``directory`` matching ``pattern``, raising if none."""
    matches = sorted(directory.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"Could not find LaSRC aux {description} matching '{pattern}' "
            f"in {directory}. Check LASRC_AUX_DIR and its layout."
        )
    return matches


def _ambiguous_error(
    directory: Path, pattern: str, description: str, matches: list[Path]
) -> ValueError:
    return ValueError(
        f"Ambiguous LaSRC aux {description}: multiple files match "
        f"'{pattern}' in {directory}: {[m.name for m in matches]}"
    )


def _glob_one(directory: Path, pattern: str, description: str) -> Path:
    """Return the single file in ``directory`` matching ``pattern``.

    Raises if zero or more than one match (ambiguous aux data).
    """
    matches = _glob_any(directory, pattern, description)
    if len(matches) > 1:
        raise _ambiguous_error(directory, pattern, description, matches)
    return matches[0]


def _viirs_processed_at(path: Path) -> dt.datetime | None:
    """Parse the processing time from a VIIRS LADS file name.

    Names look like ``<PRODUCT>.A<YYYYDDD>.<VERSION>.<YYYYDDDHHMMSS>.h5``
    (e.g. ``VNP04ANC.A2025201.002.2025206043958.h5``). Returns None if the
    name does not follow that layout.
    """
    parts = path.name.split(".")
    if len(parts) != 5:
        return None
    try:
        return dt.datetime.strptime(parts[3], _VIIRS_PROCESSED_AT_FORMAT)
    except ValueError:
        return None


def _latest_processed_viirs(
    directory: Path, pattern: str, description: str, matches: list[Path]
) -> Path:
    """Pick the most recently processed VIIRS LADS file among ``matches``.

    Raises the usual ambiguity error if any candidate lacks a parseable
    processing time, since there is then no safe way to order them.
    """
    processed = {m: _viirs_processed_at(m) for m in matches}
    if any(ts is None for ts in processed.values()):
        raise _ambiguous_error(directory, pattern, description, matches)

    chosen = max(matches, key=lambda m: (processed[m], m.name))
    logger.warning(
        "Multiple LaSRC aux %s files match '%s' in %s: %s. "
        "Using most recently processed file %s (processed at %s).",
        description,
        pattern,
        directory,
        [m.name for m in matches],
        chosen.name,
        processed[chosen],
    )
    return chosen


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

    If several VIIRS files match (e.g. both SNPP ``VNP04ANC`` and NOAA-20
    ``VJ104ANC``, or reprocessed versions), the most recently processed one
    is used.
    """
    year = acquisition.strftime("%Y")
    doy = acquisition.strftime("%j")
    lads_year_dir = _require(aux_dir / "LADS" / year, f"LADS directory for {year}")

    if aux_source == "VIIRS":
        # e.g. VJ104ANC.A2026073.001.h5 / VNP04ANC.A2026073...
        pattern = f"V*04ANC.A{year}{doy}.*"
    else:  # MODIS, e.g. MOD04... / MYD04...
        pattern = f"M*{year}{doy}*"

    description = f"{aux_source} daily water-vapor/ozone (DOY {doy})"
    if aux_source != "VIIRS":
        return _glob_one(lads_year_dir, pattern, description)

    matches = _glob_any(lads_year_dir, pattern, description)
    if len(matches) == 1:
        return matches[0]
    return _latest_processed_viirs(lads_year_dir, pattern, description, matches)
