"""Helpers for the standalone LaSRC pipeline's surface-reflectance outputs.

Kept dependency-free (no imports from the rest of the ``lasrc`` package) so both
``lasrc.sentinel`` and ``lasrc.landsat`` can import it without an import cycle
through ``lasrc.workflow``.
"""

from __future__ import annotations

# Both the C (espa-surface-reflectance) and Rust (lasrc-rs) LaSRC name their
# final surface-reflectance products "<scene_id>_sr_band<N>" and
# "<scene_id>_sr_aerosol[_qa]". The debug upload keeps only these (the working
# directory is otherwise full of inputs and conversion intermediates).
_SR_PRODUCT_MARKERS = ("_sr_band", "_sr_aerosol")


def is_sr_product(name: str) -> bool:
    """True if ``name`` is a LaSRC surface-reflectance product file."""
    return any(marker in name for marker in _SR_PRODUCT_MARKERS)
