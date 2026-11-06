"""Helpers for the standalone ``fmask`` pipeline's outputs.

Kept free of imports from the rest of the ``fmask`` package so both
``fmask.sentinel`` and ``fmask.landsat`` can import it without an import cycle
through ``fmask.workflow``.
"""

from __future__ import annotations

from importlib.metadata import version

from hls_workflows.common import S3Path

# Both the Landsat and Sentinel-2 ``RunFmaskV5`` tasks run the "UPL" model, and
# Fmask names its mask "<scene>_UPL.tif". The ENVI ``fmask.bin`` is a plain
# gdal_translate copy of it, so only the TIF is uploaded.
FMASK_PRODUCT_SUFFIX = "_UPL.tif"


def is_fmask_product(name: str) -> bool:
    """True if ``name`` is the Fmask cloud mask GeoTIFF."""
    return name.endswith(FMASK_PRODUCT_SUFFIX)


def fmask_version() -> str:
    """Installed ``fmask`` Python package version (PEP 440 normalized)."""
    return version("fmask")


def debug_upload_base(bucket: str, prefix: str, granule_id: str) -> S3Path:
    """Debug-bucket location for one granule's Fmask outputs.

    Keyed by the installed Fmask version so runs of different Fmask builds do
    not overwrite each other: ``<prefix>/<fmask version>/<granule_id>/``.
    """
    return S3Path(bucket, f"{prefix}/{fmask_version()}/{granule_id}")
