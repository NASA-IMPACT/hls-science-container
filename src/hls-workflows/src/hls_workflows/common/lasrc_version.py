"""Version strings recorded for the selected LaSRC implementation."""

from __future__ import annotations

import os

from hls_workflows.constants import LASRC_VERSION


def ac_code(lasrc_version: LASRC_VERSION) -> str:
    """AC code recorded in products: the lasrc-rs version, or ``$ACCODE`` for C."""
    if lasrc_version == "rust":
        import lasrc

        return f"LaSRC v{lasrc.ALGORITHM_VERSION}"
    return os.environ["ACCODE"]


def espa_app_version() -> str:
    """``app_version`` recorded on bands registered from lasrc-rs output."""
    import lasrc

    return f"LaSRC_{lasrc.ALGORITHM_VERSION} (Collection 2)"
