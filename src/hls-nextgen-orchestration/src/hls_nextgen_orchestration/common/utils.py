from __future__ import annotations

import logging
import os
import shlex
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_LOG_COMMANDS = os.getenv("HLS_LOG_COMMANDS", "1") != "0"
_LOG_OUTPUT = os.getenv("HLS_LOG_OUTPUT", "0") == "1"


def run_command(
    cmd: Sequence[str | Path],
    *,
    check: bool = True,
    log_command: bool = _LOG_COMMANDS,
    log_output: bool = _LOG_OUTPUT,
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    if log_command:
        logger.info("+ %s", shlex.join(str(c) for c in cmd))
    result = subprocess.run(cmd, check=check, **kwargs)
    if log_output:
        if result.stdout:
            logger.info("[stdout]\n%s", result.stdout)
        if result.stderr:
            logger.info("[stderr]\n%s", result.stderr)
    return result
