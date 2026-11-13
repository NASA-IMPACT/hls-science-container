"""Access to the real L30/S30 fixture granules under ``tests/data``."""

import json
import shutil
from pathlib import Path
from typing import Any

DATA = Path(__file__).parent / "data"
L30_GRANULE = "HLS.L30.T56NQK.2026309T235813.v2.0"
S30_GRANULE = "HLS.S30.T55GEM.2026309T235229.v2.0"
GRANULES = {"L30": L30_GRANULE, "S30": S30_GRANULE}


def copy_granule(product: str, root: Path) -> Path:
    """Copy a real fixture granule into ``root/<granule>/``; return that dir."""
    dest = root / GRANULES[product]
    shutil.copytree(DATA / product, dest)
    return dest


def edit_json(path: Path, **updates: Any) -> None:
    """Overwrite top-level keys of a JSON file."""
    doc = json.loads(path.read_text())
    doc.update(updates)
    path.write_text(json.dumps(doc))
