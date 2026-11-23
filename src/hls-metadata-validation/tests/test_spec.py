import json
from pathlib import Path
from typing import Any

import pytest

from hls_metadata_validation.spec import SpecError, load_spec_file

SZA_ASSET: dict[str, Any] = {
    "dtype": "uint16",
    "nodata": 40000,
    "scale_factor": 0.01,
    "add_offset": 0.0,
    "stac_asset": "SZA",
    "granule_tags": False,
}

MINIMAL_SPEC: dict[str, Any] = {
    "product": "X30",
    "version": "2.0",
    "default_tolerance": 1e-6,
    "attributes": {
        "ACCODE": {
            "required": True,
            "type": "string",
            "cog_tag": "ACCODE",
            "cmr_additional_attribute": "ACCODE",
            "golden": "volatile",
        }
    },
    "assets": {"SZA": SZA_ASSET},
    "known_gaps": [],
}


def _write(tmp_path: Path, doc: dict[str, Any], name: str = "X.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(doc))
    return p


def test_spec_unknown_field_raises(tmp_path: Path) -> None:
    bad = json.loads(json.dumps(MINIMAL_SPEC))
    bad["assets"]["SZA"]["no_data"] = 0
    with pytest.raises(SpecError):
        load_spec_file(_write(tmp_path, bad))


def test_include_list_merges_with_inline(tmp_path: Path) -> None:
    _write(tmp_path, {"SZA": SZA_ASSET}, "angles.json")
    doc = {
        **MINIMAL_SPEC,
        "assets": {
            "$include": ["angles.json"],
            "B01": {"dtype": "int16", "nodata": -9999},
        },
    }
    assert set(load_spec_file(_write(tmp_path, doc)).assets) == {"SZA", "B01"}


def test_include_duplicate_key_raises(tmp_path: Path) -> None:
    _write(tmp_path, {"SZA": SZA_ASSET}, "angles.json")
    doc = {**MINIMAL_SPEC, "assets": {"$include": "angles.json", "SZA": SZA_ASSET}}
    with pytest.raises(SpecError):
        load_spec_file(_write(tmp_path, doc))
