"""Product metadata spec model and loader."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, cast

import jsonschema

AttributeType = Literal["string", "number", "integer", "datetime"]
GoldenMode = Literal["exact", "approx", "volatile"]
Surface = Literal["cog", "cmr", "stac", "golden", "spec"]

INCLUDE_KEY = "$include"


class SpecError(Exception):
    """Raised when a spec document is malformed."""


@dataclass(frozen=True)
class AttributeSpec:
    name: str
    required: bool
    type: AttributeType
    cog_tag: str | None
    cmr_additional_attribute: str | None
    stac_property: str | None
    value_from: str | None
    golden: GoldenMode
    tolerance: float | None


@dataclass(frozen=True)
class AssetSpec:
    name: str
    file_suffix: str
    dtype: str
    nodata: float | int
    scale_factor: float | None
    add_offset: float | None
    stac_asset: str | None
    granule_tags: bool = True


@dataclass(frozen=True)
class KnownGap:
    id: str
    surface: Surface
    check: str
    target: str | None
    reason: str

    def matches(self, surface: str, check: str, target: str) -> bool:
        if self.surface != surface or self.check != check:
            return False
        return self.target is None or target in self.target.split("|")


@dataclass(frozen=True)
class ProductSpec:
    product: str
    version: str
    attributes: dict[str, AttributeSpec]
    assets: dict[str, AssetSpec]
    known_gaps: tuple[KnownGap, ...]
    default_tolerance: float


def _specs_dir() -> Path:
    return Path(str(files("hls_metadata_validation") / "specs"))


def _schema() -> dict[str, Any]:
    return cast(
        dict[str, Any], json.loads((_specs_dir() / "spec.schema.json").read_text())
    )


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise SpecError(f"cannot read spec file {path}: {e}") from e


def _resolve(node: Any, base: Path) -> Any:
    """Replace ``{"$include": ...}`` objects with the merged referenced files."""
    if isinstance(node, list):
        return [_resolve(item, base) for item in node]
    if not isinstance(node, dict):
        return node
    if INCLUDE_KEY not in node:
        return {k: _resolve(v, base) for k, v in node.items()}

    includes = node[INCLUDE_KEY]
    if isinstance(includes, str):
        includes = [includes]
    merged: dict[str, Any] = {}
    sources: list[tuple[str, dict[str, Any]]] = []
    for rel in includes:
        path = (base / rel).resolve()
        content = _resolve(_read_json(path), path.parent)
        if not isinstance(content, dict):
            raise SpecError(f"included file {path} is not a JSON object")
        sources.append((str(path), content))
    inline = {k: _resolve(v, base) for k, v in node.items() if k != INCLUDE_KEY}
    sources.append(("inline", inline))
    for origin, content in sources:
        for key, value in content.items():
            if key in merged:
                raise SpecError(f"duplicate key {key!r} from {origin}")
            merged[key] = value
    return merged


def _build(doc: dict[str, Any]) -> ProductSpec:
    attributes = {
        name: AttributeSpec(
            name=name,
            required=a.get("required", True),
            type=a.get("type", "string"),
            cog_tag=a.get("cog_tag"),
            cmr_additional_attribute=a.get("cmr_additional_attribute"),
            stac_property=a.get("stac_property"),
            value_from=a.get("value_from"),
            golden=a.get("golden", "exact"),
            tolerance=a.get("tolerance"),
        )
        for name, a in doc["attributes"].items()
    }
    assets = {
        name: AssetSpec(
            name=name,
            file_suffix=a.get("file_suffix", f"{name}.tif"),
            dtype=a["dtype"],
            nodata=a["nodata"],
            scale_factor=a.get("scale_factor"),
            add_offset=a.get("add_offset"),
            stac_asset=a.get("stac_asset"),
            granule_tags=a.get("granule_tags", True),
        )
        for name, a in doc["assets"].items()
    }
    gaps = tuple(
        KnownGap(
            id=g["id"],
            surface=g["surface"],
            check=g["check"],
            target=g.get("target"),
            reason=g["reason"],
        )
        for g in doc["known_gaps"]
    )
    for attr in attributes.values():
        if attr.value_from is not None:
            asset_name = attr.value_from.split(".", 1)[0]
            if asset_name not in assets:
                raise SpecError(
                    f"attribute {attr.name}: value_from refers to unknown asset "
                    f"{asset_name!r}"
                )
    return ProductSpec(
        product=doc["product"],
        version=doc["version"],
        attributes=attributes,
        assets=assets,
        known_gaps=gaps,
        default_tolerance=float(doc["default_tolerance"]),
    )


def load_spec_file(path: Path) -> ProductSpec:
    """Load, resolve includes in, and validate a spec file."""
    path = Path(path)
    doc = _resolve(_read_json(path), path.parent)
    try:
        jsonschema.validate(doc, _schema())
    except jsonschema.ValidationError as e:
        loc = "/".join(str(p) for p in e.absolute_path) or "<root>"
        raise SpecError(f"{path}: {loc}: {e.message}") from e
    return _build(doc)


def load_spec(product: str) -> ProductSpec:
    """Load the packaged spec for ``product`` (e.g. ``"L30"``)."""
    path = _specs_dir() / f"{product}.json"
    if not path.is_file():
        raise SpecError(f"no packaged spec for product {product!r}")
    return load_spec_file(path)
