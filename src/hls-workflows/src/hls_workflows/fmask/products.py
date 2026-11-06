"""Helpers for the standalone Fmask pipeline's outputs.

Kept dependency-free so both ``fmask.sentinel`` and ``fmask.landsat`` can import
it without an import cycle through ``fmask.workflow``.
"""

from __future__ import annotations

# Both the Landsat and Sentinel-2 ``RunFmaskV5`` tasks run the "UPL" model, and
# Fmask names its mask "<scene>_UPL.tif". The ENVI ``fmask.bin`` is a plain
# gdal_translate copy of it, so only the TIF is uploaded.
FMASK_PRODUCT_SUFFIX = "_UPL.tif"


def is_fmask_product(name: str) -> bool:
    """True if ``name`` is the Fmask cloud mask GeoTIFF."""
    return name.endswith(FMASK_PRODUCT_SUFFIX)
