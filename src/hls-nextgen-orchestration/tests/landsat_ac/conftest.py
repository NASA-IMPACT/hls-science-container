from collections.abc import Callable
from pathlib import Path

import pytest

from tests.mock_cli import (
    cli_noop,
    cli_touch_flag_arg,
    cli_touch_last_arg,
    cli_touch_nth_arg,
    make_python_script,
)

# Mocks for external CLI tools used in the Landsat pipeline.

DOWNLOAD_LANDSAT = make_python_script("""
import os
import sys
from pathlib import Path

bucket, prefix, dest_dir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
dest_dir.mkdir(parents=True, exist_ok=True)
granule = os.environ.get("GRANULE", "LC08_L1TP_000000_20200101_20200114_01_T1")
(dest_dir / f"{granule}_MTL.txt").touch()
print(granule)
""")

CHECK_SOLAR_ZENITH_LANDSAT = cli_noop("valid")

RUN_FMASK = cli_noop()

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

CREATE_LANDSAT_SR_HDF_XML = cli_touch_nth_arg(2)

CONVERT_ESPA_TO_HDF = cli_touch_flag_arg("--hdf")

LANDSAT_ADD_FMASK_SDS = cli_touch_last_arg()

SCRIPTS = {
    "download_landsat": DOWNLOAD_LANDSAT,
    "check_solar_zenith_landsat": CHECK_SOLAR_ZENITH_LANDSAT,
    "run_Fmask.sh": RUN_FMASK,
    "fmask": FMASK_V5,
    "gdal_translate": GDAL_TRANSLATE,
    "convert_lpgs_to_espa": CONVERT_LPGS_TO_ESPA,
    "do_lasrc_landsat.py": DO_LASRC_LANDSAT,
    "create_landsat_sr_hdf_xml": CREATE_LANDSAT_SR_HDF_XML,
    "convert_espa_to_hdf": CONVERT_ESPA_TO_HDF,
    "landsat-add-fmask-sds": LANDSAT_ADD_FMASK_SDS,
}


@pytest.fixture
def mock_binaries(install_mock_binaries: Callable[[dict[str, str]], Path]) -> Path:
    """
    Installs the Landsat specific mock binaries.
    """
    return install_mock_binaries(SCRIPTS)
