"""Compare fresh outputs against reference (golden) outputs."""

from __future__ import annotations

import math
from collections.abc import Callable
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

from hls_metadata_validation.cmr import normalize_cmr
from hls_metadata_validation.discover import GranuleFiles, discover, load_cog_doc
from hls_metadata_validation.findings import Finding, Report
from hls_metadata_validation.runner import safe_load, validate_dir
from hls_metadata_validation.spec import AttributeSpec, ProductSpec, load_spec
from hls_metadata_validation.stac import load_stac

BUILTIN_VOLATILE = (
    "cmr.granule.InsertTime",
    "cmr.granule.LastUpdate",
    "cmr.granule.DataGranule.ProductionDateTime",
    "cmr.granule.DataGranule.DataGranuleSizeInBytes",
    "stac.assets.*.href",
    "stac.links",
    "stac.properties.created",
    "stac.properties.updated",
)


class GoldenRules:
    """Per-path comparison rules derived from a product spec."""

    def __init__(self, spec: ProductSpec) -> None:
        self.spec = spec
        self._patterns: list[tuple[str, AttributeSpec]] = []
        for attr in spec.attributes.values():
            if attr.cog_tag is not None:
                self._patterns.append((f"*.tags.{attr.cog_tag}", attr))
            if attr.cmr_additional_attribute is not None:
                self._patterns.append(
                    (f"cmr.additional_attributes.{attr.cmr_additional_attribute}", attr)
                )
            if attr.stac_property is not None:
                self._patterns.append((f"stac.properties.{attr.stac_property}", attr))

    def _attribute(self, path: str) -> AttributeSpec | None:
        for pattern, attr in self._patterns:
            if fnmatchcase(path, pattern):
                return attr
        return None

    def is_volatile(self, path: str) -> bool:
        if any(fnmatchcase(path, p) for p in BUILTIN_VOLATILE):
            return True
        attr = self._attribute(path)
        return attr is not None and attr.golden == "volatile"

    def tolerance_for(self, path: str) -> tuple[float, float]:
        """Return ``(relative, absolute)`` tolerance for numeric values."""
        attr = self._attribute(path)
        absolute = attr.tolerance if attr is not None and attr.tolerance else 0.0
        return self.spec.default_tolerance, absolute


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def compare_docs(
    new: Any,
    ref: Any,
    *,
    path: str,
    rules: GoldenRules,
    granule: str = "",
    target: str | None = None,
) -> list[Finding]:
    """Recursively compare ``new`` against ``ref``."""
    tgt = path if target is None else target

    def err(check: str, message: str) -> Finding:
        return Finding("error", "golden", check, granule, tgt, f"{path}: {message}")

    if rules.is_volatile(path):
        return []

    if isinstance(new, dict) and isinstance(ref, dict):
        findings = [
            err("key.extra", f"key {k!r} not in reference") for k in new if k not in ref
        ]
        findings += [
            err("key.missing", f"key {k!r} missing") for k in ref if k not in new
        ]
        for key in new:
            if key in ref:
                findings += compare_docs(
                    new[key],
                    ref[key],
                    path=f"{path}.{key}",
                    rules=rules,
                    granule=granule,
                    target=target,
                )
        return findings

    if isinstance(new, list) and isinstance(ref, list):
        if len(new) != len(ref):
            return [err("length", f"{len(new)} item(s), reference has {len(ref)}")]
        findings = []
        for a, b in zip(new, ref, strict=True):
            findings += compare_docs(
                a, b, path=path, rules=rules, granule=granule, target=target
            )
        return findings

    fa, fb = _as_float(new), _as_float(ref)
    if fa is not None and fb is not None:
        rel, absolute = rules.tolerance_for(path)
        if math.isclose(fa, fb, rel_tol=rel, abs_tol=absolute):
            return []
        return [err("value", f"{new!r} != reference {ref!r}")]

    if new != ref:
        return [err("value", f"{new!r} != reference {ref!r}")]
    return []


def _compare_granule(
    g: GranuleFiles, ref: GranuleFiles, rules: GoldenRules
) -> list[Finding]:
    def file_missing(target: str) -> Finding:
        return Finding(
            "error",
            "golden",
            "reference.file_missing",
            g.granule,
            target,
            "file present in new outputs but not in the reference",
        )

    findings: list[Finding] = []

    def compare(
        loader: Callable[[Path], Any],
        new_path: Path,
        ref_path: Path,
        *,
        path: str,
        target: str | None,
        file_target: str,
    ) -> None:
        docs = []
        for p in (new_path, ref_path):
            doc, problem = safe_load(
                loader, p, granule=g.granule, target=file_target, surface="golden"
            )
            if problem is not None:
                findings.append(problem)
            docs.append(doc)
        if docs[0] is not None and docs[1] is not None:
            findings.extend(
                compare_docs(
                    docs[0],
                    docs[1],
                    path=path,
                    rules=rules,
                    granule=g.granule,
                    target=target,
                )
            )

    for asset in sorted(g.cogs):
        if asset not in ref.cogs:
            findings.append(file_missing(asset))
            continue
        compare(
            load_cog_doc,
            g.cogs[asset],
            ref.cogs[asset],
            path=asset,
            target=asset,
            file_target=asset,
        )
    for name, new_path, ref_path, loader, prefix in (
        ("cmr.xml", g.cmr, ref.cmr, normalize_cmr, "cmr"),
        ("stac.json", g.stac, ref.stac, load_stac, "stac"),
    ):
        if new_path is None:
            continue
        if ref_path is None:
            findings.append(file_missing(name))
            continue
        compare(loader, new_path, ref_path, path=prefix, target=None, file_target=name)
    return findings


def golden_dir(new_dir: Path, ref_dir: Path) -> Report:
    """Validate ``new_dir`` against the spec, then compare it to ``ref_dir``."""
    report = validate_dir(new_dir)
    refs = {g.granule: g for g in discover(ref_dir)}
    specs: dict[str, ProductSpec] = {}
    for g in discover(new_dir):
        spec = specs.setdefault(g.product, load_spec(g.product))
        ref = refs.get(g.granule)
        if ref is None:
            report.add(
                Finding(
                    "warning",
                    "golden",
                    "reference.skipped",
                    g.granule,
                    str(ref_dir),
                    "no reference outputs for this granule",
                )
            )
            continue
        sub = Report(_compare_granule(g, ref, GoldenRules(spec)))
        sub.apply_known_gaps(spec)
        report.extend(sub.findings)
    new_granules = {g.granule for g in discover(new_dir)}
    for name in sorted(set(refs) - new_granules):
        report.add(
            Finding(
                "error",
                "golden",
                "reference.unmatched",
                name,
                str(new_dir),
                "reference granule has no counterpart in the new outputs",
            )
        )
    return report
