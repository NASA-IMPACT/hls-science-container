from __future__ import annotations

import shutil


def validate_command(command: str) -> None:
    """
    Checks if a command exists on the PATH.

    Parameters
    ----------
    command : str
        The name of the command executable to check.

    Raises
    ------
    RuntimeError
        If the command is not found on the PATH.
    """
    if not shutil.which(command):
        raise RuntimeError(f"Required command '{command}' not found on PATH.")
