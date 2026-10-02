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

# Mocks for landsat_tile pipeline

EXTRACT_LANDSAT_HMS = cli_noop("101010")

LANDSAT_TILE = cli_touch_last_arg()

LANDSAT_ANGLE_TILE = cli_touch_last_arg()

LANDSAT_NBAR = cli_noop()

HDF_TO_COG = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("--output-dir", required=True)
parser.add_argument("--product")
args = parser.parse_args()

output_dir = Path(args.output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
(output_dir / "dummy.tif").touch()
""")

CREATE_THUMBNAIL = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-i")
parser.add_argument("-o", required=True)
parser.add_argument("-s")
args = parser.parse_args()

Path(args.o).touch()
""")

CREATE_METADATA = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("input", nargs="?")
parser.add_argument("--save", required=True)
args = parser.parse_args()

Path(args.save).touch()
""")

CMR_TO_STAC_ITEM = cli_touch_nth_arg(2)

CREATE_MANIFEST = cli_touch_nth_arg(2)

GRANULE_TO_GIBS = make_python_script("""
import sys
from pathlib import Path

input_dir, output_dir, output_name = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
gibs_dir = output_dir / "GIBS_ID_1"
gibs_dir.mkdir(parents=True, exist_ok=True)
(gibs_dir / "test.xml").touch()
(gibs_dir / "test.tif").touch()
""")

VI_GENERATE_INDICES = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-i")
parser.add_argument("-o", required=True)
parser.add_argument("-s")
args = parser.parse_args()

output_dir = Path(args.o)
output_dir.mkdir(parents=True, exist_ok=True)
(output_dir / "NDVI.tif").touch()
""")

VI_GENERATE_METADATA = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-i")
parser.add_argument("-o", required=True)
args = parser.parse_args()

output_dir = Path(args.o)
output_dir.mkdir(parents=True, exist_ok=True)
(output_dir / "VI.cmr.xml").touch()
""")

VI_GENERATE_STAC_ITEMS = cli_touch_flag_arg("--out_json")

SCRIPTS = {
    "extract_landsat_hms.py": EXTRACT_LANDSAT_HMS,
    "landsat-tile": LANDSAT_TILE,
    "landsat-angle-tile": LANDSAT_ANGLE_TILE,
    "landsat-nbar": LANDSAT_NBAR,
    "hdf_to_cog": HDF_TO_COG,
    "create_thumbnail": CREATE_THUMBNAIL,
    "create_metadata": CREATE_METADATA,
    "cmr_to_stac_item": CMR_TO_STAC_ITEM,
    "create_manifest": CREATE_MANIFEST,
    "granule_to_gibs": GRANULE_TO_GIBS,
    "vi_generate_indices": VI_GENERATE_INDICES,
    "vi_generate_metadata": VI_GENERATE_METADATA,
    "vi_generate_stac_items": VI_GENERATE_STAC_ITEMS,
}


@pytest.fixture
def mock_binaries(install_mock_binaries: Callable[[dict[str, str]], Path]) -> Path:
    """
    Installs the Landsat Tile specific mock binaries.
    """
    return install_mock_binaries(SCRIPTS)
