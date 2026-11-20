import dataclasses
import json
import shutil
from pathlib import Path

import pytest
from fixtures import L30_GRANULE, S30_GRANULE, copy_granule, edit_json

from hls_metadata_validation.golden import GoldenRules, compare_docs, golden_dir
from hls_metadata_validation.spec import KnownGap, ProductSpec, load_spec


@pytest.fixture
def rules(l30: ProductSpec) -> GoldenRules:
    return GoldenRules(l30)


def _ref_copy(new: Path, tmp_path: Path) -> Path:
    ref = tmp_path / "ref"
    shutil.copytree(new, ref)
    return ref


def _stac(root: Path) -> Path:
    return root / S30_GRANULE / f"{S30_GRANULE}_stac.json"


def test_extra_key_fails(rules: GoldenRules) -> None:
    fs = compare_docs({"a": 1, "b": 2}, {"a": 1}, path="x", rules=rules)
    assert [(f.check, f.severity) for f in fs] == [("key.extra", "error")]


def test_approx_within_absolute_tolerance(rules: GoldenRules) -> None:
    path = "cmr.additional_attributes.CLOUD_COVERAGE"
    assert compare_docs(["10.4"], ["10"], path=path, rules=rules) == []
    assert [f.check for f in compare_docs(["12"], ["10"], path=path, rules=rules)] == [
        "value"
    ]


def test_volatile_only_presence(rules: GoldenRules) -> None:
    assert (
        compare_docs(
            "2026-01-01", "2025-01-01", path="SZA.tags.HLS_PROCESSING_TIME", rules=rules
        )
        == []
    )
    assert (
        compare_docs("v2", "v1", path="cmr.additional_attributes.ACCODE", rules=rules)
        == []
    )
    assert (
        compare_docs(
            "a",
            "b",
            path="stac.properties.processing:software.Atmospheric Correction",
            rules=rules,
        )
        == []
    )


def test_identical_dirs_pass(s30_dir: Path, tmp_path: Path) -> None:
    r = golden_dir(s30_dir, _ref_copy(s30_dir, tmp_path))
    assert r.ok, r.to_text()


def test_reference_granule_missing_is_skipped_warning(
    two_granule_dir: Path, tmp_path: Path
) -> None:
    ref = tmp_path / "ref"
    copy_granule("S30", ref)
    r = golden_dir(two_granule_dir, ref)
    skipped = [f for f in r.findings if f.check == "reference.skipped"]
    assert [(f.granule, f.severity) for f in skipped] == [(L30_GRANULE, "warning")]


def test_known_gap_downgrades_golden_difference(
    s30_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _ref_copy(s30_dir, tmp_path)
    edit_json(ref / S30_GRANULE / f"{S30_GRANULE}.SZA.tif.json", nodata=None)
    spec = load_spec("S30")
    gap = KnownGap(
        id="test-gap", surface="golden", check="value", target="SZA", reason="test"
    )
    patched = dataclasses.replace(spec, known_gaps=(gap,))
    monkeypatch.setattr("hls_metadata_validation.golden.load_spec", lambda _: patched)
    r = golden_dir(s30_dir, ref)
    assert r.ok, r.to_text()
    assert [f.known_gap for f in r.findings if f.surface == "golden"] == ["test-gap"]


def test_non_angle_difference_fails(s30_dir: Path, tmp_path: Path) -> None:
    ref = _ref_copy(s30_dir, tmp_path)
    stac = _stac(ref)
    doc = json.loads(stac.read_text())
    doc["properties"]["eo:cloud_cover"] += 50
    stac.write_text(json.dumps(doc))
    r = golden_dir(s30_dir, ref)
    assert not r.ok
    targets = {f.target for f in r.findings if f.surface == "golden"}
    assert "stac.properties.eo:cloud_cover" in targets


def test_cmr_insert_time_is_volatile(s30_dir: Path, tmp_path: Path) -> None:
    ref = _ref_copy(s30_dir, tmp_path)
    cmr = ref / S30_GRANULE / f"{S30_GRANULE}.cmr.xml"
    text = cmr.read_text()
    assert "<InsertTime>2026-11-11" in text
    cmr.write_text(
        text.replace("<InsertTime>2026-11-11", "<InsertTime>2020-01-01").replace(
            "<LastUpdate>2026-11-11", "<LastUpdate>2020-01-01"
        )
    )
    r = golden_dir(s30_dir, ref)
    assert r.ok, r.to_text()


def test_reference_granule_without_new_counterpart_is_error(
    s30_dir: Path, tmp_path: Path
) -> None:
    ref = _ref_copy(s30_dir, tmp_path)
    copy_granule("L30", ref)
    r = golden_dir(s30_dir, ref)
    assert not r.ok
    assert any(
        f.check == "reference.unmatched"
        and f.granule == L30_GRANULE
        and f.severity == "error"
        for f in r.findings
    )
