from collections.abc import Callable
from pathlib import Path

import pytest

# Mocks for landsat_tile pipeline

EXTRACT_LANDSAT_HMS = """#!/bin/bash
# usage: extract_landsat_hms.py file.hdf
# Returns a time string like "101010"
echo "101010"
"""

LANDSAT_TILE = """#!/bin/bash
# usage: landsat-tile mgrs ulx uly ... ac_file output_file
# Last arg is output
dest="${!#}"
touch "$dest"
"""

LANDSAT_ANGLE_TILE = """#!/bin/bash
# usage: landsat-angle-tile ... ... ... output_file
dest="${!#}"
touch "$dest"
"""

LANDSAT_NBAR = """#!/bin/bash
# usage: landsat-nbar nbar_input nbar_angle nbar_cfactor
# Does not create a new file name, modifies inputs or creates implicit files.
# The python task renames the input files, so we assume this just runs.
echo "Running NBAR"
"""

HDF_TO_COG = """#!/bin/bash
# usage: hdf_to_cog input --output-dir dir ...
# Creates dummy TIFs
dir="$3"
if [ "$2" == "--output-dir" ]; then
    touch "$dir/dummy.tif"
fi
"""

CREATE_THUMBNAIL = """#!/bin/bash
# usage: create_thumbnail -i ... -o output.jpg ...
# Find -o flag
while getopts ":i:o:s:" opt; do
  case $opt in
    o)
      touch "$OPTARG"
      ;;
  esac
done
"""

CREATE_METADATA = """#!/bin/bash
# usage: create_metadata input --save output.xml
while [[ $# -gt 0 ]]; do
  case $1 in
    --save)
      touch "$2"
      shift # past value
      shift # past arg
      ;;
    *)
      shift
      ;;
  esac
done
"""

CMR_TO_STAC_ITEM = """#!/bin/bash
# usage: cmr_to_stac_item input.xml output.json ...
touch "$2"
"""

CREATE_MANIFEST = """#!/bin/bash
# usage: create_manifest dir output.json ...
touch "$2"
"""

GRANULE_TO_GIBS = """#!/bin/bash
# usage: granule_to_gibs input_dir output_dir output_name
out_dir="$2"
# Create a fake gibs ID folder and an xml inside
mkdir -p "$out_dir/GIBS_ID_1"
touch "$out_dir/GIBS_ID_1/test.xml"
touch "$out_dir/GIBS_ID_1/test.tif"
"""

VI_GENERATE_INDICES = """#!/bin/bash
# usage: vi_generate_indices -i ... -o out_dir ...
while getopts ":i:o:s:" opt; do
  case $opt in
    o)
      mkdir -p "$OPTARG"
      touch "$OPTARG/NDVI.tif"
      ;;
  esac
done
"""

VI_GENERATE_METADATA = """#!/bin/bash
# usage: vi_generate_metadata -i ... -o out_dir
while getopts ":i:o:" opt; do
  case $opt in
    o)
      mkdir -p "$OPTARG"
      touch "$OPTARG/VI.cmr.xml"
      ;;
  esac
done
"""

VI_GENERATE_STAC_ITEMS = """#!/bin/bash
# usage: ... --out_json output.json
while [[ $# -gt 0 ]]; do
  case $1 in
    --out_json)
      touch "$2"
      shift
      shift
      ;;
    *)
      shift
      ;;
  esac
done
"""

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
