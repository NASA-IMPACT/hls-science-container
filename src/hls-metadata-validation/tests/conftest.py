from pathlib import Path

import pytest
from fixtures import DATA, L30_GRANULE, copy_granule

from hls_metadata_validation.spec import ProductSpec, load_spec


@pytest.fixture
def l30() -> ProductSpec:
    return load_spec("L30")


@pytest.fixture
def l30_cmr_path() -> Path:
    return DATA / "L30" / f"{L30_GRANULE}.cmr.xml"


@pytest.fixture
def l30_stac_path() -> Path:
    return DATA / "L30" / f"{L30_GRANULE}_stac.json"


@pytest.fixture
def s30_dir(tmp_path: Path) -> Path:
    """A products dir holding the real S30 fixture granule (passes the spec)."""
    copy_granule("S30", tmp_path / "products")
    return tmp_path / "products"


@pytest.fixture
def two_granule_dir(s30_dir: Path) -> Path:
    """The S30 products dir plus the real L30 granule and an unrelated file."""
    copy_granule("L30", s30_dir)
    (s30_dir / "notes.txt").write_text("unrelated")
    return s30_dir
