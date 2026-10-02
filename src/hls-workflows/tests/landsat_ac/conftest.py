import os
from collections.abc import Callable
from pathlib import Path

import pytest

from hls_workflows.landsat_ac import tasks
from tests.mock_cli import (
    cli_noop,
    cli_touch_flag_arg,
    cli_touch_last_arg,
    make_python_script,
)

# Mocks for external CLI tools used in the Landsat pipeline.

FMASK_V5 = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--imagepath", "-i", required=True)
parser.add_argument("--model", default="UPL")
parser.add_argument("--dcloud", type=int, default=3)
parser.add_argument("--dshadow", type=int, default=5)
args = parser.parse_args()

image_dir = Path(args.imagepath)
(image_dir / f"{image_dir.name}_{args.model}.tif").touch()
""")

GDAL_TRANSLATE = cli_touch_last_arg()

CONVERT_LPGS_TO_ESPA = make_python_script("""
import os
from pathlib import Path

granule = os.environ.get("GRANULE", "LC08_L1TP_000000_20200101_20200114_01_T1")
Path(f"{granule}.xml").touch()
""")

DO_LASRC_LANDSAT = cli_noop()

CONVERT_ESPA_TO_HDF = cli_touch_flag_arg("--hdf")

LANDSAT_ADD_FMASK_SDS = cli_touch_last_arg()

SCRIPTS = {
    "fmask": FMASK_V5,
    "gdal_translate": GDAL_TRANSLATE,
    "convert_lpgs_to_espa": CONVERT_LPGS_TO_ESPA,
    "do_lasrc_landsat.py": DO_LASRC_LANDSAT,
    "convert_espa_to_hdf": CONVERT_ESPA_TO_HDF,
    "landsat-add-fmask-sds": LANDSAT_ADD_FMASK_SDS,
}


@pytest.fixture
def mock_hls_utilities(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the hls-utilities functions used by the Landsat tasks."""

    def _get_landsat(bucket: str, path: str, output_directory: Path | str) -> str:
        dest_dir = Path(output_directory)
        dest_dir.mkdir(parents=True, exist_ok=True)
        granule = os.environ.get("GRANULE", "LC08_L1TP_000000_20200101_20200114_01_T1")
        (dest_dir / f"{granule}_MTL.txt").touch()
        return granule

    def _create_landsat_sr_hdf_xml(
        input_xml: Path | str, output_xml: Path | str
    ) -> None:
        Path(output_xml).touch()

    monkeypatch.setattr(tasks, "get_landsat", _get_landsat)
    monkeypatch.setattr(tasks, "solar_zenith_is_valid", lambda mtl: True)
    monkeypatch.setattr(tasks, "create_landsat_sr_hdf_xml", _create_landsat_sr_hdf_xml)


@pytest.fixture
def mock_binaries(
    install_mock_binaries: Callable[[dict[str, str]], Path],
    mock_hls_utilities: None,
) -> Path:
    """
    Installs the Landsat specific mock binaries and mocks hls-utilities functions.
    """
    return install_mock_binaries(SCRIPTS)
