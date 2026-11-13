"""Run spec validation over HLS product directories."""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path

from hls_metadata_validation.cmr import check_cmr, normalize_cmr
from hls_metadata_validation.cog import check_cog
from hls_metadata_validation.discover import (
    COG_DUMP_SUFFIX,
    GranuleFiles,
    discover,
    load_cog_doc,
)
from hls_metadata_validation.findings import Finding, FindingSurface, Report
from hls_metadata_validation.spec import ProductSpec, load_spec
from hls_metadata_validation.stac import check_stac, load_stac


def safe_load[T](
    loader: Callable[[Path], T],
    path: Path,
    *,
    granule: str,
    target: str,
    surface: FindingSurface,
) -> tuple[T | None, Finding | None]:
    """Run ``loader(path)``; turn read/parse failures into a finding."""
    try:
        return loader(path), None
    except (OSError, ValueError) as e:
        return None, Finding(
            "error",
            surface,
            "file.unreadable",
            granule,
            target,
            f"cannot read {path.name}: {e}",
        )


def validate_granule(g: GranuleFiles, spec: ProductSpec) -> list[Finding]:
    """Check every surface of one granule against ``spec``."""

    def missing(target: str, what: str) -> Finding:
        return Finding(
            "error", "spec", "file.required", g.granule, target, f"{what} not found"
        )

    findings: list[Finding] = []
    required_tags = [
        a.cog_tag
        for a in spec.attributes.values()
        if a.cog_tag is not None and a.required
    ]
    tag_types = {
        a.cog_tag: a.type for a in spec.attributes.values() if a.cog_tag is not None
    }
    for asset in spec.assets.values():
        path = g.cogs.get(asset.name)
        if path is None:
            findings.append(missing(asset.name, f"COG {asset.file_suffix}"))
            continue
        doc, problem = safe_load(
            load_cog_doc, path, granule=g.granule, target=asset.name, surface="cog"
        )
        if doc is None:
            findings.extend([problem] if problem else [])
            continue
        findings.extend(
            check_cog(
                doc,
                asset,
                granule=g.granule,
                required_tags=required_tags if asset.granule_tags else [],
                tag_types=tag_types if asset.granule_tags else {},
            )
        )
    for extra in sorted(set(g.cogs) - set(spec.assets)):
        findings.append(
            Finding(
                "warning",
                "spec",
                "file.unexpected",
                g.granule,
                extra,
                "COG asset not declared in the spec",
            )
        )

    if g.cmr is None:
        findings.append(missing("cmr.xml", "CMR XML"))
    else:
        findings.extend(check_cmr(normalize_cmr(g.cmr), spec, granule=g.granule))
    if g.stac is None:
        findings.append(missing("stac.json", "STAC Item"))
    else:
        stac_doc, problem = safe_load(
            load_stac, g.stac, granule=g.granule, target="stac.json", surface="stac"
        )
        if stac_doc is None:
            findings.extend([problem] if problem else [])
        else:
            findings.extend(check_stac(stac_doc, spec, granule=g.granule))
    if g.thumbnail is None:
        findings.append(missing("jpg", "thumbnail"))
    return findings


def validate_dir(product_dir: Path, product: str | None = None) -> Report:
    """Validate every granule found under ``product_dir``."""
    report = Report()
    granules = discover(product_dir)
    if not granules:
        report.add(
            Finding(
                "error",
                "spec",
                "discover.empty",
                "-",
                str(product_dir),
                "no HLS granule outputs found",
            )
        )
        return report
    specs: dict[str, ProductSpec] = {}
    for g in granules:
        name = product or g.product
        spec = specs.setdefault(name, load_spec(name))
        sub = Report(validate_granule(g, spec))
        sub.apply_known_gaps(spec)
        report.extend(sub.findings)
    return report


def dump_dir(product_dir: Path, out_dir: Path) -> list[Path]:
    """Write normalized COG docs and copy CMR/STAC/thumbnail per granule."""
    written: list[Path] = []
    for g in discover(product_dir):
        dest = Path(out_dir) / g.granule
        dest.mkdir(parents=True, exist_ok=True)
        for asset, path in sorted(g.cogs.items()):
            out = dest / f"{g.granule}.{asset}{COG_DUMP_SUFFIX}"
            out.write_text(
                json.dumps(load_cog_doc(path), indent=2, sort_keys=True) + "\n"
            )
            written.append(out)
        for src in (g.cmr, g.stac, g.thumbnail):
            if src is not None:
                written.append(Path(shutil.copy(src, dest / src.name)))
    return written
