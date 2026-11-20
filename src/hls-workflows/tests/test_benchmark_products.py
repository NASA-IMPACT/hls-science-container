from pathlib import Path

from tests.benchmarks.conftest import keep_products

G = "HLS.S30.T55GEM.2026309T235229.v2.0"


def test_keep_products_copies_only_final_outputs(tmp_path: Path) -> None:
    work = tmp_path / "work"
    (work / "nested").mkdir(parents=True)
    finals = [
        f"{G}.B01.tif",
        f"{G}.Fmask.tif",
        f"{G}.cmr.xml",
        f"{G}_stac.json",
        f"{G}.jpg",
    ]
    for name in finals:
        (work / name).write_text("x")
    (work / "nested" / f"{G}.SZA.tif").write_text("x")
    for name in (
        f"{G}.hdf",
        f"{G}.ANGLE.hdf",
        "HLS-VI.S30.T55GEM.2026309T235229.v2.0.NDVI.tif",
        "consolidated_sr.hdf",
    ):
        (work / name).write_text("x")

    out = tmp_path / "out"
    copied = keep_products(work, out)

    assert sorted(p.name for p in copied) == sorted([*finals, f"{G}.SZA.tif"])
    assert {p.parent for p in copied} == {out / G}


def test_keep_products_empty_work_dir(tmp_path: Path) -> None:
    (tmp_path / "work").mkdir()
    assert keep_products(tmp_path / "work", tmp_path / "out") == []
