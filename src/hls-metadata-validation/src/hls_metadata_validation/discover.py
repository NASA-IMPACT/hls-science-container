"""Find HLS granule output files in a directory tree."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from hls_metadata_validation.cog import normalize_cog

GRANULE_RE = re.compile(
    r"^(?P<granule>HLS\.(?P<product>[LS]30)\.T\w{5}\.\d{7}T\d{6}\.v\d+\.\d+)"
    r"(?P<rest>[._].+)$"
)
COG_DUMP_SUFFIX = ".tif.json"


@dataclass
class GranuleFiles:
    granule: str
    product: str
    cogs: dict[str, Path] = field(default_factory=dict)
    cmr: Path | None = None
    stac: Path | None = None
    thumbnail: Path | None = None


def _classify(g: GranuleFiles, rest: str, path: Path) -> None:
    if rest == ".cmr.xml":
        g.cmr = path
    elif rest == "_stac.json":
        g.stac = path
    elif rest == ".jpg":
        g.thumbnail = path
    elif rest.startswith(".") and rest.endswith(COG_DUMP_SUFFIX):
        asset = rest[1 : -len(COG_DUMP_SUFFIX)]
        g.cogs.setdefault(asset, path)
    elif rest.startswith(".") and rest.endswith(".tif"):
        g.cogs[rest[1:-4]] = path


def discover(product_dir: Path) -> list[GranuleFiles]:
    """Group HLS output files under ``product_dir`` by granule."""
    granules: dict[str, GranuleFiles] = {}
    paths = sorted(
        (p for p in Path(product_dir).rglob("*") if p.is_file()),
        key=lambda p: p.name.endswith(COG_DUMP_SUFFIX),
    )
    for path in paths:
        m = GRANULE_RE.match(path.name)
        if m is None:
            continue
        g = granules.setdefault(
            m["granule"], GranuleFiles(granule=m["granule"], product=m["product"])
        )
        _classify(g, m["rest"], path)
    return [granules[k] for k in sorted(granules)]


def load_cog_doc(path: Path) -> dict[str, Any]:
    """Load a normalized COG doc from a ``.tif`` or a ``.tif.json`` dump."""
    if path.name.endswith(COG_DUMP_SUFFIX):
        return cast(dict[str, Any], json.loads(path.read_text()))
    return normalize_cog(path)
