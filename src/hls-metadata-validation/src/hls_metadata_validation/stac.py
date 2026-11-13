"""Check STAC Items with pystac and against the product spec."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, cast

import pystac
from pystac.errors import STACValidationError
from pystac.validation import GetSchemaError, validate_dict

from hls_metadata_validation.findings import Finding
from hls_metadata_validation.spec import ProductSpec

MISSING = object()


def load_stac(path: Path) -> dict[str, Any]:
    """Load a STAC Item JSON file."""
    return cast(dict[str, Any], json.loads(Path(path).read_text()))


def get_property(properties: dict[str, Any], path: str) -> Any:
    """Look up ``"a.b"`` style paths; the first segment may contain ``:``."""
    node: Any = properties
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return MISSING
        node = node[part]
    return node


def _pystac_findings(doc: dict[str, Any], granule: str) -> list[Finding]:
    def err(check: str, message: str) -> Finding:
        return Finding("error", "stac", check, granule, "stac.json", message)

    try:
        pystac.Item.from_dict(copy.deepcopy(doc), preserve_dict=True)
    except Exception as e:
        return [err("schema", f"not a valid STAC Item: {e}")]
    try:
        validate_dict(doc)
    except STACValidationError as e:
        return [err("schema", str(e))]
    except GetSchemaError as e:
        return [err("schema.unavailable", f"cannot fetch a declared schema: {e}")]
    return []


def check_stac(
    doc: dict[str, Any], spec: ProductSpec, *, granule: str
) -> list[Finding]:
    """Check a STAC Item against core/extension schemas and the product spec."""

    def err(check: str, target: str, message: str) -> Finding:
        return Finding("error", "stac", check, granule, target, message)

    if not isinstance(doc, dict):
        return [err("doc.type", "stac.json", "STAC Item is not a JSON object")]
    for key in ("properties", "assets"):
        if not isinstance(doc.get(key, {}), dict):
            return [err("doc.type", key, f"{key!r} is not a JSON object")]

    findings = _pystac_findings(doc, granule)

    properties = doc.get("properties", {})
    for attr in spec.attributes.values():
        if attr.stac_property is None:
            continue
        value = get_property(properties, attr.stac_property)
        if attr.required and (value is MISSING or value is None or value == ""):
            findings.append(
                err(
                    "property.required", attr.stac_property, "required property missing"
                )
            )

    assets = doc.get("assets", {})
    for asset in spec.assets.values():
        if asset.stac_asset is None:
            continue
        stac_asset = assets.get(asset.stac_asset)
        if stac_asset is None:
            findings.append(err("asset.required", asset.stac_asset, "asset missing"))
            continue
        if "raster:bands" not in stac_asset:
            findings.append(
                err(
                    "asset.raster",
                    asset.stac_asset,
                    "asset has no raster:bands (nodata/scale/offset)",
                )
            )
    return findings
