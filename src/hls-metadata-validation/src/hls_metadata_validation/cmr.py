"""Normalize and check CMR ECHO10 granule XML."""

from __future__ import annotations

import math
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any

from lxml import etree

from hls_metadata_validation.findings import Finding
from hls_metadata_validation.spec import ProductSpec
from hls_metadata_validation.valuetypes import matches_type


@cache
def _schema() -> etree.XMLSchema:
    xsd = Path(str(files("metadata_creator") / "templates" / "Granule.xsd"))
    return etree.XMLSchema(etree.parse(str(xsd)))


def _element_to_obj(el: etree._Element) -> Any:
    children = [c for c in el if isinstance(c.tag, str)]
    if not children:
        return (el.text or "").strip()
    out: dict[str, Any] = {}
    for child in children:
        value = _element_to_obj(child)
        tag = str(child.tag)
        if tag in out:
            if not isinstance(out[tag], list):
                out[tag] = [out[tag]]
            out[tag].append(value)
        else:
            out[tag] = value
    return out


def normalize_cmr(path: Path) -> dict[str, Any]:
    try:
        tree = etree.parse(str(path))
    except (OSError, etree.XMLSyntaxError) as e:
        return {
            "granule_ur": None,
            "xsd_errors": [f"cannot parse XML: {e}"],
            "additional_attributes": {},
            "granule": {},
        }
    root = tree.getroot()
    schema = _schema()
    schema.validate(tree)
    attributes: dict[str, list[str]] = {}
    for attr in root.findall("./AdditionalAttributes/AdditionalAttribute"):
        name = (attr.findtext("Name") or "").strip()
        values = [
            (v.text or "").strip()
            for v in attr.findall("./Values/Value")
            if (v.text or "").strip()
        ]
        attributes[name] = values
    granule = _element_to_obj(root)
    if isinstance(granule, dict):
        granule.pop("AdditionalAttributes", None)
    else:
        granule = {}
    return {
        "granule_ur": root.findtext("GranuleUR"),
        "xsd_errors": [
            f"line {e.line}: {e.message}"
            for e in schema.error_log  # type: ignore[attr-defined]
        ],
        "additional_attributes": attributes,
        "granule": granule,
    }


def resolve_value_from(spec: ProductSpec, ref: str) -> float | int | str | None:
    """Resolve a ``"<asset>.<field>"`` reference against the spec's assets."""
    asset_name, field = ref.split(".", 1)
    value = getattr(spec.assets[asset_name], field)
    if value is None or isinstance(value, (int, float, str)):
        return value
    raise TypeError(f"{ref} resolves to unsupported type {type(value).__name__}")


def check_cmr(doc: dict[str, Any], spec: ProductSpec, *, granule: str) -> list[Finding]:
    def err(check: str, target: str, message: str) -> Finding:
        return Finding("error", "cmr", check, granule, target, message)

    findings = [err("xsd", "Granule.xsd", e) for e in doc.get("xsd_errors", [])]
    present: dict[str, list[str]] = doc.get("additional_attributes", {})
    for attr in spec.attributes.values():
        name = attr.cmr_additional_attribute
        if name is None:
            continue
        values = present.get(name, [])
        if not values:
            if attr.required:
                findings.append(
                    err("attr.required", name, "required AdditionalAttribute missing")
                )
            continue
        bad = [v for v in values if not matches_type(v, attr.type)]
        if bad:
            findings.append(
                err("attr.type", name, f"value(s) {bad!r} are not of type {attr.type}")
            )
            continue
        if attr.value_from is None:
            continue
        expected = resolve_value_from(spec, attr.value_from)
        if expected is None:
            continue
        try:
            matches = math.isclose(
                float(values[0]), float(expected), rel_tol=1e-9, abs_tol=1e-12
            )
        except ValueError:
            matches = False
        if not matches:
            findings.append(
                err(
                    "attr.value",
                    name,
                    f"value {values[0]!r}, expected {expected} ({attr.value_from})",
                )
            )
    return findings
