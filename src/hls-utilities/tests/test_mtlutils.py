import datetime as dt
from pathlib import Path
from typing import IO, Any

import pytest

from hls_utilities.vendored import mtlutils
from hls_utilities.vendored.mtlutils import MTLParseError, parsemeta

TEST_DATA = Path(__file__).parent / "data"
MTL_C1 = TEST_DATA / "LC08_L1TP_027039_20190901_20190901_01_RT_MTL.txt"

MTL_TEXT = """GROUP = L1_METADATA_FILE
  GROUP = IMAGE_ATTRIBUTES
    SUN_ELEVATION = 59.78054395
    CLOUD_COVER = 12
    DATE_ACQUIRED = 2019-09-01
    SPACECRAFT_ID = "LANDSAT_8"
  END_GROUP = IMAGE_ATTRIBUTES
END_GROUP = L1_METADATA_FILE
END
"""


def test_parsemeta_file() -> None:
    metadata = parsemeta(MTL_C1)
    attrs = metadata["L1_METADATA_FILE"]["IMAGE_ATTRIBUTES"]
    assert attrs["SUN_ELEVATION"] == pytest.approx(59.78054395)


def test_parsemeta_directory(tmp_path: Path) -> None:
    (tmp_path / "LC08_TEST_MTL.txt").write_text(MTL_TEXT)
    metadata = parsemeta(tmp_path)
    assert metadata["L1_METADATA_FILE"]["IMAGE_ATTRIBUTES"]["CLOUD_COVER"] == 12


def test_parsemeta_string() -> None:
    attrs = parsemeta(MTL_TEXT)["L1_METADATA_FILE"]["IMAGE_ATTRIBUTES"]
    assert attrs["SUN_ELEVATION"] == pytest.approx(59.78054395)
    assert attrs["CLOUD_COVER"] == 12
    assert attrs["DATE_ACQUIRED"] == dt.date(2019, 9, 1)
    assert attrs["SPACECRAFT_ID"] == "LANDSAT_8"


def test_parsemeta_lines_after_end(tmp_path: Path) -> None:
    mtl = tmp_path / "LC08_TEST_MTL.txt"
    mtl.write_text(MTL_TEXT + "\n\n\n")
    metadata = parsemeta(mtl)
    assert "L1_METADATA_FILE" in metadata


def test_parsemeta_unclosed_group(tmp_path: Path) -> None:
    mtl = tmp_path / "LC08_TEST_MTL.txt"
    mtl.write_text(MTL_TEXT.replace("END_GROUP = L1_METADATA_FILE\n", ""))
    with pytest.raises(MTLParseError):
        parsemeta(mtl)


def test_parsemeta_missing() -> None:
    with pytest.raises(MTLParseError):
        parsemeta("/does/not/exist_MTL.txt")


def test_parsemeta_closes_file(monkeypatch: pytest.MonkeyPatch) -> None:
    handles: list[IO[Any]] = []

    def _open(*args: Any, **kwargs: Any) -> IO[Any]:
        handle: IO[Any] = open(*args, **kwargs)
        handles.append(handle)
        return handle

    monkeypatch.setattr(mtlutils, "open", _open, raising=False)
    parsemeta(MTL_C1)
    assert len(handles) == 1
    assert handles[0].closed
