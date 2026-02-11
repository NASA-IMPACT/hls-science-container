"""Convenience functions for running common CLI commands"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def run_hdf_to_cog(
    input_file: Path,
    output_dir: Path,
    product: str,
    debug_mode: bool = False,
) -> None:
    """
    Runs the hdf_to_cog CLI command.

    Parameters
    ----------
    input_file : Path
        Path to the input HDF file.
    output_dir : Path
        Directory where output COGs will be saved.
    product : str
        Product type (e.g., 'S30', 'L30', 'S30_ANGLES').
    debug_mode : bool, optional
        If True, adds the --debug-mode flag and suppresses errors (check=False).
        Default is False.
    """
    cmd = [
        "hdf_to_cog",
        str(input_file),
        "--output-dir",
        str(output_dir),
        "--product",
        product,
    ]

    if debug_mode:
        cmd.append("--debug-mode")

    logger.info(f"Running hdf_to_cog: {' '.join(cmd)}")

    # In debug mode, existing logic sets check=False to allow continuation even if conversion fails
    # In production, check=True ensures integrity
    subprocess.run(cmd, check=not debug_mode)
