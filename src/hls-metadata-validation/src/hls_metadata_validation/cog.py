"""Normalize and check COG metadata."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import rasterio
from rio_cogeo.cogeo import cog_validate

from hls_metadata_validation.findings import Finding
from hls_metadata_validation.spec import AssetSpec, AttributeType
from hls_metadata_validation.valuetypes import matches_type

COG_DOC_KEYS = (
    "tags",
    "band_tags",
    "nodata",
    "scales",
    "offsets",
    "dtype",
    "count",
    "descriptions",
    "crs",
    "transform",
    "width",
    "height",
    "is_cog",
)


def normalize_cog(path: Path) -> dict[str, Any]:
    """Read the metadata of a COG into a JSON-serializable dict."""
    is_cog, _errors, _warnings = cog_validate(str(path), quiet=True)
    with rasterio.open(path) as src:
        return {
            "tags": dict(src.tags()),
            "band_tags": [dict(src.tags(i)) for i in src.indexes],
            "nodata": src.nodata,
            "scales": [float(s) for s in src.scales],
            "offsets": [float(o) for o in src.offsets],
            "dtype": src.dtypes[0],
            "count": src.count,
            "descriptions": list(src.descriptions),
            "crs": src.crs.to_string() if src.crs else None,
            "transform": [float(v) for v in list(src.transform)[:6]],
            "width": src.width,
            "height": src.height,
            "is_cog": bool(is_cog),
        }


def _close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)


def check_cog(
    doc: dict[str, Any],
    asset: AssetSpec,
    *,
    granule: str,
    required_tags: Iterable[str],
    tag_types: Mapping[str, AttributeType] | None = None,
) -> list[Finding]:
    """Check a normalized COG doc against the asset spec."""

    def err(check: str, message: str, target: str = asset.name) -> Finding:
        return Finding("error", "cog", check, granule, target, message)

    missing = [k for k in COG_DOC_KEYS if k not in doc]
    if missing:
        return [err("doc.key", f"normalized COG doc lacks {k!r}") for k in missing]

    findings: list[Finding] = []
    if doc["dtype"] != asset.dtype:
        findings.append(err("dtype", f"dtype {doc['dtype']}, expected {asset.dtype}"))

    nodata = doc["nodata"]
    if nodata is None:
        findings.append(err("nodata", f"nodata not set, expected {asset.nodata}"))
    elif not _close(float(nodata), float(asset.nodata)):
        findings.append(err("nodata", f"nodata {nodata}, expected {asset.nodata}"))

    for check, values, expected, default in (
        ("scale", doc["scales"], asset.scale_factor, 1.0),
        ("offset", doc["offsets"], asset.add_offset, 0.0),
    ):
        want = default if expected is None else expected
        if not values:
            findings.append(err(check, f"no {check} reported, expected {want}"))
        elif not _close(float(values[0]), want):
            findings.append(err(check, f"{check} {values[0]}, expected {want}"))

    if not doc["is_cog"]:
        findings.append(err("is_cog", "file is not a valid Cloud Optimized GeoTIFF"))

    tags = doc["tags"]
    for tag in required_tags:
        if tag not in tags or str(tags[tag]).strip() == "":
            findings.append(
                err(
                    "tag.required",
                    f"required tag {tag!r} missing",
                    f"{asset.name}:{tag}",
                )
            )
    for tag, type_ in (tag_types or {}).items():
        if tag in tags and not matches_type(str(tags[tag]), type_):
            findings.append(
                err(
                    "tag.type",
                    f"tag value {tags[tag]!r} is not of type {type_}",
                    f"{asset.name}:{tag}",
                )
            )
    return findings
