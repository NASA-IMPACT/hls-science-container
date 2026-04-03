from collections.abc import Callable
from pathlib import Path

import pytest

# Mocks for external CLI tools used in the Landsat pipeline.

DOWNLOAD_LANDSAT = """#!/bin/bash
# usage: download_landsat bucket prefix dest_dir
mkdir -p "$3"
G=${GRANULE:-LC08_L1TP_000000_20200101_20200114_01_T1}
touch "$3/${G}_MTL.txt"
echo $G
"""

CHECK_SOLAR_ZENITH_LANDSAT = """#!/bin/bash
echo "valid"
"""

RUN_FMASK = """#!/bin/bash
echo "Mock Fmask running"
"""

FMASK_V5 = """#!/bin/bash
# usage: fmask --imagepath <dir> --model UPL
prev=""
for arg in "$@"; do
    if [[ "$prev" == "--imagepath" || "$prev" == "-i" ]]; then
        imagepath="${arg%/}"
    fi
    prev="$arg"
done
basename=$(basename "$imagepath")
touch "${imagepath}/${basename}_UPL.tif"
echo "Fmask v5 complete"
"""

GDAL_TRANSLATE = """#!/bin/bash
# The destination is always the last argument
dest="${!#}"
touch "$dest"
"""

CONVERT_LPGS_TO_ESPA = """#!/bin/bash
G=${GRANULE:-LC08_L1TP_000000_20200101_20200114_01_T1}
touch "${G}.xml"
"""

DO_LASRC_LANDSAT = """#!/bin/bash
echo "Mock LaSRC running"
"""

CREATE_LANDSAT_SR_HDF_XML = """#!/bin/bash
touch "$2"
"""

CONVERT_ESPA_TO_HDF = """#!/bin/bash
for arg in "$@"; do
    if [[ "$arg" == --hdf=* ]]; then
        touch "${arg#*=}"
    fi
done
"""

LANDSAT_ADD_FMASK_SDS = """#!/bin/bash
dest="${!#}"
touch "$dest"
"""

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
