from __future__ import annotations

import shutil
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.sentinel.assets import EnvConfig


# --- Mock CLI Scripts for Sentinel-2 ---
def make_script(command: str) -> str:
    """Wrap CLI commands in a Bash script with error handling"""
    return f"""#!/bin/bash
set -eux
{command}
"""


CHECK_SZA = make_script('echo "valid"')

CHECK_SENTINEL_CLOUDS = make_script('echo "valid"')

RUN_FMASK_SH_CLEAR = make_script("""
# usage: run_Fmask.sh (runs in cwd)
touch "granuleid_Fmask4.tif"
echo "Fmask 4.7 finished (0.42 minutes)\nfor S2C_SCENE with 96.3% clear pixels\n"
""")

RUN_FMASK_SH_CLOUDY = make_script("""
# usage: run_Fmask.sh (runs in cwd)
touch "granuleid_Fmask4.tif"
echo "Fmask 4.7 finished (0.42 minutes)\nfor S2C_SCENE with 1.2% clear pixels\n"
""")

PARSE_FMASK = make_script("""
# usage: parse_fmask <text>
clear=$(echo $@ | sed -n 's/.*with \\([0-9.]*\\).*/\\1/p')
if [[ $clear < 2 ]]; then
    echo "invalid"
else
    echo "valid"
fi
""")

SENTINEL_DERIVE_ANGLE = make_script("""
# usage: sentinel-derive-angle ... output
# Grab the last argument as the output file
out="${@: -1}"
touch "$out"
echo "Derive angles complete: $out"
""")

FMASK_V5 = make_script("""
# usage: fmask --imagepath <dir> --model UPL [--print_summary yes]
prev=""
for arg in "$@"; do
    if [[ "$prev" == "--imagepath" || "$prev" == "-i" ]]; then
        imagepath="${arg%/}"
    fi
    prev="$arg"
done
basename=$(basename "$imagepath" .SAFE)
touch "${imagepath}/${basename}_UPL.tif"
echo "Clear: 96.3%"
""")

GDAL_TRANSLATE = make_script("""
# usage: gdal_translate ... input output
out="${@: -1}"
touch "$out"
echo "GDAL translate complete: $out"
""")

APPLY_S2_QUALITY_MASK = make_script("""
# usage: apply_s2_quality_mask directory
echo "Applied S2 quality mask in $1"
""")

UNPACKAGE_S2 = make_script("""
# usage: unpackage_s2.py -i input -o output
echo "Unpackage S2 complete"
""")

CONVERT_SENTINEL_TO_ESPA = make_script("""
# usage: convert_sentinel_to_espa
touch "S2A_TEST.xml"
echo "Convert to ESPA complete"
""")

DO_LASRC_SENTINEL = make_script("""
# usage: do_lasrc_sentinel.py --xml mtd_file
touch $(pwd)/S2A_TEST_sr_aerosol_qa.img
touch $(pwd)/S2A_TEST_sr_band5.img
echo "LaSRC complete"
""")

CREATE_SR_HDF_XML = make_script("""
# usage: create_sr_hdf_xml xml hls_xml suffix
touch "$2"
echo "Created SR HDF XML: $2"
""")

CONVERT_ESPA_TO_HDF = make_script("""
# usage: convert_espa_to_hdf --xml=x --hdf=h
for arg in "$@"; do
  if [[ $arg == --hdf=* ]]; then
    hdf="${arg#*=}"
    touch $hdf
  fi
done
echo "Convert ESPA to HDF complete for $hdf"
""")

SENTINEL_TWOHDF2ONE = make_script("""
# usage: sentinel-twohdf2one ... output
out="${@: -1}"
touch "$out"
echo "Combined HDFs: $out"
""")

SENTINEL_ADD_FMASK_SDS = make_script("""
# usage: sentinel-add-fmask-sds ... output
out="${@: -1}"
touch "$out"
echo "Added Fmask SDS: $out"
""")

SENTINEL_TRIM = make_script("""
# usage: sentinel-trim input
# modifies in place or assumes output exists
echo "Trim complete: $1"
""")

SENTINEL_CREATE_S2AT30M = make_script("""
# usage: sentinel-create-s2at30m in out
touch "$2"
touch "${2%.*}.hdf.hdr"
echo "Resample complete: $2"
""")

CONSOLIDATE_SR = make_script("""
# usage: sentinel-consolidate *input-sr output-sr
inputs=("${@:1:$#-1}")
output="${!#}"
for file in "${inputs[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo "Input file $file doesn't exist."
        exit 1
    fi
done
touch "$output"
echo "Consolidated SR to $output"
""")

CONSOLIDATE_ANGLE = make_script("""
# usage: sentinel-consolidate-angle *input-angles output-angle
inputs=("${@:1:$#-1}")
output="${!#}"
for file in "${inputs[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo "Input file $file doesn't exist."
        exit 1
    fi
done
touch "$output"
echo "Consolidated angles to $output"
""")

SENTINEL_DERIVE_NBAR = make_script("""
# usage: sentinel-derive-nbar inp angle cfactor
echo "NBAR derive complete"
""")

SENTINEL_L8_LIKE = make_script("""
# usage: sentinel-l8-like param input
# this updates the `input` in place
input=$2
if [[ -f $input ]]; then
    echo "L8-like complete for $input"
    touch $input
else
    echo "Cannot find input!"
    exit 1
fi
""")

HDF_TO_COG = make_script("""
# usage: hdf_to_cog <input> --output-dir=<dir> --product=<product>
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      output_dir="$2"
      shift 2
      ;;
    --product)
      product="$2"
      shift 2
      ;;
    -*)
      echo "Unknown option: $1"
      exit 1
      ;;
    *)
      # Everything else is treated as the <input>
      input="$1"
      shift
      ;;
  esac
done

echo "Converting to COG: $input"

bname=$(basename $input .hdf)
if [[ "$product" == "S30" ]]; then
    band=B05
elif [[ "$product" == "S30_ANGLES" ]]; then
    band=VZA
    # remove `.ANGLE` suffix
    bname=$(basename $bname .ANGLE)
else
    echo "Unsupported product $product"
    exit 1
fi
touch $output_dir/${bname}.${band}.tif
""")

CREATE_THUMBNAIL = make_script("""
# usage: create_thumbnail -i dir -o out ...
while getopts "i:o:s:" opt; do
  case $opt in
    o) output_file="$OPTARG" ;;
  esac
done
touch "$output_file"
echo "Created thumbnail"
""")

CREATE_METADATA = make_script("""
# usage: create_metadata input --save output
output="${@: -1}"
touch "$output"
echo "Created metadata: $output"
""")

CMR_TO_STAC_ITEM = make_script("""
# usage: cmr_to_stac_item xml json ...
touch "$2"
echo "Created STAC JSON: $2"
""")

CREATE_MANIFEST = make_script("""
# usage: create_manifest dir output ...
touch "$2"
echo "Created manifest: $2"
""")

GRANULE_TO_GIBS = make_script("""
# usage: granule_to_gibs working_dir gibs_dir base_name
gibs_dir="$2"
base_name="$3"
# Create a fake gibs structure
mkdir -p "$gibs_dir/${base_name}_GIBS_ID"
touch "$gibs_dir/${base_name}_GIBS_ID/${base_name}.xml"
touch "$gibs_dir/${base_name}_GIBS_ID/${base_name}.tif"
echo "Generated GIBS tiles"
""")

VI_GENERATE_INDICES = make_script("""
# usage: vi_generate_indices -i in -o out -s base_name
while getopts "i:o:s:" opt; do
  case $opt in
    o) output_dir="$OPTARG" ;;
    s) base_name="$OPTARG" ;;
  esac
done
mkdir -p "$output_dir"
touch "$output_dir/${base_name}_NDVI.tif"
echo "Generated VI indices"
""")

# FIXME: generate metadata
VI_GENERATE_METADATA = make_script("""
# usage: vi_generate_metadata -i in -o out
while getopts "i:o:" opt; do
  case $opt in
    o) output_dir="$OPTARG" ;;
  esac
done

echo "Generated VI metadata"
""")

VI_GENERATE_STAC_ITEMS = make_script("""
# usage: vi_generate_stac_items ... --out_json output
output="${@: -1}"
touch "$output"
echo "Generated VI STAC: $output"
""")


SENTINEL_SCRIPTS = {
    # ----- sentinel_granule.sh
    "check_solar_zenith_sentinel": CHECK_SZA,
    "gdal_translate": GDAL_TRANSLATE,
    "apply_s2_quality_mask": APPLY_S2_QUALITY_MASK,
    "sentinel-derive-angle": SENTINEL_DERIVE_ANGLE,
    "check_sentinel_clouds": CHECK_SENTINEL_CLOUDS,
    "run_Fmask.sh": RUN_FMASK_SH_CLEAR,
    "parse_fmask": PARSE_FMASK,
    "fmask": FMASK_V5,
    "unpackage_s2.py": UNPACKAGE_S2,
    "convert_sentinel_to_espa": CONVERT_SENTINEL_TO_ESPA,
    "do_lasrc_sentinel.py": DO_LASRC_SENTINEL,
    "create_sr_hdf_xml": CREATE_SR_HDF_XML,
    "convert_espa_to_hdf": CONVERT_ESPA_TO_HDF,
    "sentinel-twohdf2one": SENTINEL_TWOHDF2ONE,
    "sentinel-add-fmask-sds": SENTINEL_ADD_FMASK_SDS,
    "sentinel-trim": SENTINEL_TRIM,
    # ----- sentinel.sh
    "sentinel-consolidate": CONSOLIDATE_SR,
    "sentinel-consolidate-angle": CONSOLIDATE_ANGLE,
    "sentinel-create-s2at30m": SENTINEL_CREATE_S2AT30M,
    "sentinel-derive-nbar": SENTINEL_DERIVE_NBAR,
    "sentinel-l8-like": SENTINEL_L8_LIKE,
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
    Installs Sentinel-specific mock binaries.
    Uses `install_mock_binaries` from the root conftest.py.
    """
    return install_mock_binaries(SENTINEL_SCRIPTS)


@pytest.fixture
def sentinel_config(tmp_path: Path) -> EnvConfig:
    """Provides a valid EnvConfig for testing."""
    working_dir = tmp_path / "working"
    working_dir.mkdir()

    return EnvConfig(
        job_id="test-job",
        input_bucket="test-input-bucket",
        output_bucket="test-output-bucket",
        gibs_bucket="test-gibs-bucket",
        granule_ids=["S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"],
        working_dir=working_dir,
        ac_code="LaSRC v3.5.1.8",
        replace_existing=False,
    )


@pytest.fixture
def populate_sentinel_safe() -> Callable[[Path, Sentinel2Granule], Path]:
    """
    Fixture that returns a function to create a simulated zipped Sentinel SAFE file.
    Includes essential metadata XMLs and the detector footprint needed for angles.
    """

    def _populate(root_dir: Path, granule: Sentinel2Granule) -> Path:
        safe_name = f"{granule.to_str()}.SAFE"
        safe_dir = root_dir / safe_name
        safe_dir.mkdir(parents=True, exist_ok=True)

        acquisition_time_str = granule.acquisition_time.strftime("%Y%m%dT%H%M%S")
        inner_granule_id = f"L1C_{granule.tile_id}_A000000_{acquisition_time_str}"

        # Create inner structure
        granule_inner = safe_dir / "GRANULE" / inner_granule_id
        granule_inner.mkdir(parents=True, exist_ok=True)
        qi_data = granule_inner / "QI_DATA"
        qi_data.mkdir(parents=True, exist_ok=True)

        # Fake some expected files
        (safe_dir / "MTD_MSIL1C.xml").touch()
        (granule_inner / "MTD_TL.xml").touch()
        (qi_data / "MSK_DETFOO_B06.jp2").touch()

        # Zip it
        zip_path = root_dir / f"{granule.to_str()}.zip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in safe_dir.rglob("*"):
                # We only want to zip files, directories are implied
                if file_path.is_file():
                    # Calculate path relative to root_dir so that the zip starts with SAFE_NAME
                    arcname = file_path.relative_to(root_dir)
                    zf.write(file_path, arcname)

        # Cleanup directory
        shutil.rmtree(safe_dir)

        return Path(zip_path)

    return _populate
