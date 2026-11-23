import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from fixtures import DATA, L30_GRANULE, S30_GRANULE

from hls_metadata_validation.cli import main
from hls_metadata_validation.runner import validate_dir

ANGLES = ("SZA", "SAA", "VZA", "VAA")


def test_real_s30_passes() -> None:
    r = validate_dir(DATA / "S30")
    assert r.ok, r.to_text()


def test_real_l30_fails_only_on_angle_nodata_and_scale() -> None:
    r = validate_dir(DATA / "L30")
    errs = {(f.check, f.target) for f in r.findings if f.severity == "error"}
    assert errs == {(c, a) for c in ("nodata", "scale") for a in ANGLES}


def test_findings_grouped_per_granule(two_granule_dir: Path) -> None:
    r = validate_dir(two_granule_dir)
    assert {f.granule for f in r.findings if f.severity == "error"} == {L30_GRANULE}


def test_missing_and_corrupt_files_are_errors_not_crashes(s30_dir: Path) -> None:
    g = s30_dir / S30_GRANULE
    (g / f"{S30_GRANULE}.VAA.tif.json").unlink()
    (g / f"{S30_GRANULE}_stac.json").write_text('{"truncated": ')
    (g / f"{S30_GRANULE}.B01.tif").write_bytes(b"not a tiff")
    found = {(f.check, f.target) for f in validate_dir(s30_dir).findings}
    assert {
        ("file.required", "VAA"),
        ("file.unreadable", "stac.json"),
        ("file.unreadable", "B01"),
    } <= found


def test_cli_spec_exit_codes_and_json(s30_dir: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    assert runner.invoke(main, ["spec", str(s30_dir)]).exit_code == 0
    report = tmp_path / "r.json"
    res = runner.invoke(main, ["spec", str(DATA / "L30"), "--json", str(report)])
    assert res.exit_code == 1
    assert json.loads(report.read_text())["ok"] is False


@pytest.mark.parametrize("sep", ["; ", " + "])
def test_multi_scene_values_type_checked_per_value(s30_dir: Path, sep: str) -> None:
    granule = s30_dir / S30_GRANULE
    b01 = granule / f"{S30_GRANULE}.B01.tif.json"
    cmr = granule / f"{S30_GRANULE}.cmr.xml"
    doc = json.loads(b01.read_text())
    time = doc["tags"]["SENSING_TIME"]
    cmr_text = cmr.read_text()
    assert cmr_text.count(f"<Value>{time}</Value>") == 1

    def write(second: str) -> None:
        doc["tags"]["SENSING_TIME"] = f"{time}{sep}{second}"
        b01.write_text(json.dumps(doc))
        # metadata_creator leaves "+"-joined values as one <Value>
        cmr.write_text(
            cmr_text.replace(
                f"<Value>{time}</Value>", f"<Value>{time}{sep}{second}</Value>"
            )
        )

    write(time)
    r = validate_dir(s30_dir)
    assert r.ok, r.to_text()

    write("not-a-time")
    r = validate_dir(s30_dir)
    errs = {(f.check, f.target) for f in r.findings if f.severity == "error"}
    assert errs == {("tag.type", "B01:SENSING_TIME"), ("attr.type", "SENSING_TIME")}
