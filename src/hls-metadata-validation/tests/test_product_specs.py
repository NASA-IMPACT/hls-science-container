from importlib.resources import files
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "needle", ['"nodata": 40000', '"nodata": -9999', '"scale_factor": 0.0001']
)
def test_encoding_facts_written_once(needle: str) -> None:
    root = Path(str(files("hls_metadata_validation") / "specs"))
    hits = [p for p in root.rglob("*.json") if needle in p.read_text()]
    assert len(hits) == 1, hits
