"""Factory helpers for generating fake Python CLI scripts used in tests.

Each factory returns a string containing a complete, executable Python script
(with a ``#!/usr/bin/env python3`` shebang). Scripts are installed as mock
binaries on PATH by the ``install_mock_binaries`` fixture in conftest.py.

Using Python means argument parsing is handled by ``argparse``, which accepts
both ``--flag value`` and ``--flag=value`` transparently — eliminating the
class of parsing bugs that bash-based mocks are prone to.
"""

from __future__ import annotations


def make_python_script(body: str) -> str:
    """Wrap a Python script body with a shebang line."""
    return f"#!/usr/bin/env python3\n{body}\n"


def cli_noop(stdout: str = "") -> str:
    """CLI that exits 0, optionally printing a single line to stdout."""
    body = f"print({stdout!r})" if stdout else ""
    return make_python_script(body)


def cli_touch_last_arg() -> str:
    """CLI that touches ``sys.argv[-1]`` as a path."""
    return make_python_script(
        "import sys\nfrom pathlib import Path\nPath(sys.argv[-1]).touch()\n"
    )


def cli_touch_nth_arg(n: int) -> str:
    """CLI that touches ``sys.argv[n]`` as a path (standard 1-based argv indexing)."""
    return make_python_script(
        f"import sys\nfrom pathlib import Path\nPath(sys.argv[{n}]).touch()\n"
    )


def cli_touch_flag_arg(flag: str) -> str:
    """CLI that touches the path given by ``--flag=<path>`` or ``--flag <path>``.

    Any other arguments are accepted and ignored, so the mock stays valid as
    the real CLI evolves.
    """
    return make_python_script(
        "import argparse\n"
        "from pathlib import Path\n"
        "parser = argparse.ArgumentParser()\n"
        f"parser.add_argument({flag!r}, required=True)\n"
        "args, _ = parser.parse_known_args()\n"
        f"Path(getattr(args, {flag.lstrip('-').replace('-', '_')!r})).touch()\n"
    )
