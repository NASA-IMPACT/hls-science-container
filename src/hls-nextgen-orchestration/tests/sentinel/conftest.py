from __future__ import annotations

import shutil
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.sentinel.assets import EnvConfig
from tests.mock_cli import (
    cli_noop,
    cli_touch_flag_arg,
    cli_touch_last_arg,
    cli_touch_nth_arg,
    make_python_script,
)

# --- Mock CLI Scripts for Sentinel-2 ---

CHECK_SZA = cli_noop("valid")

CHECK_SENTINEL_CLOUDS = cli_noop("valid")

RUN_FMASK_SH_CLEAR = make_python_script("""
from pathlib import Path
Path("granuleid_Fmask4.tif").touch()
print("Fmask 4.7 finished (0.42 minutes)")
print("for S2C_SCENE with 96.3% clear pixels")
""")

RUN_FMASK_SH_CLOUDY = make_python_script("""
from pathlib import Path
Path("granuleid_Fmask4.tif").touch()
print("Fmask 4.7 finished (0.42 minutes)")
print("for S2C_SCENE with 1.2% clear pixels")
""")

PARSE_FMASK = make_python_script("""
import re
import sys

text = " ".join(sys.argv[1:])
match = re.search(r"([0-9.]+)%", text)
value = float(match.group(1)) if match else 50.0
print("invalid" if value < 2 else "valid")
""")

SENTINEL_DERIVE_ANGLE = cli_touch_last_arg()

FMASK_V5 = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--imagepath", "-i", required=True)
parser.add_argument("--model", default="UPL")
parser.add_argument("--print_summary", default="no")
parser.add_argument("--dcloud", type=int, default=3)
parser.add_argument("--dshadow", type=int, default=5)
args = parser.parse_args()

safe_dir = Path(args.imagepath)
(safe_dir / f"{safe_dir.stem}_{args.model}.tif").touch()
if args.print_summary == "yes":
    print("Summary: Cloud = 2.50%, Shadow = 1.20%, Snow = 0.00%, Clear = 96.30%")
""")

GDAL_TRANSLATE = cli_touch_last_arg()

APPLY_S2_QUALITY_MASK = cli_noop()

UNPACKAGE_S2 = cli_noop()

CONVERT_SENTINEL_TO_ESPA = make_python_script("""
from pathlib import Path
Path("S2A_TEST.xml").touch()
""")

DO_LASRC_SENTINEL = make_python_script("""
from pathlib import Path
cwd = Path.cwd()
(cwd / "S2A_TEST_sr_aerosol_qa.img").touch()
(cwd / "S2A_TEST_sr_band5.img").touch()
""")

CREATE_SR_HDF_XML = cli_touch_nth_arg(2)

CONVERT_ESPA_TO_HDF = cli_touch_flag_arg("--hdf")

SENTINEL_TWOHDF2ONE = cli_touch_last_arg()

SENTINEL_ADD_FMASK_SDS = cli_touch_last_arg()

SENTINEL_TRIM = cli_noop()

SENTINEL_CREATE_S2AT30M = make_python_script("""
import sys
from pathlib import Path

output = Path(sys.argv[2])
output.touch()
Path(str(output.with_suffix("")) + ".hdf.hdr").touch()
""")

CONSOLIDATE_SR = make_python_script("""
import sys
from pathlib import Path

*inputs, output = sys.argv[1:]
for f in inputs:
    if not Path(f).exists():
        print(f"Input file {f} doesn't exist.", file=__import__("sys").stderr)
        raise SystemExit(1)
Path(output).touch()
""")

CONSOLIDATE_ANGLE = make_python_script("""
import sys
from pathlib import Path

*inputs, output = sys.argv[1:]
for f in inputs:
    if not Path(f).exists():
        print(f"Input file {f} doesn't exist.", file=__import__("sys").stderr)
        raise SystemExit(1)
Path(output).touch()
""")

SENTINEL_DERIVE_NBAR = cli_noop()

SENTINEL_L8_LIKE = make_python_script("""
import sys
from pathlib import Path

input_path = Path(sys.argv[2])
if not input_path.exists():
    print(f"Cannot find input: {input_path}", file=__import__("sys").stderr)
    raise SystemExit(1)
input_path.touch()
""")

HDF_TO_COG = make_python_script("""
import argparse
import sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("--output-dir", required=True)
parser.add_argument("--product", required=True)
args = parser.parse_args()

bname = Path(args.input).stem
output_dir = Path(args.output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

if args.product == "S30":
    band = "B05"
elif args.product == "S30_ANGLES":
    band = "VZA"
    bname = Path(bname).stem  # strip .ANGLE
else:
    print(f"Unsupported product {args.product}", file=sys.stderr)
    raise SystemExit(1)

(output_dir / f"{bname}.{band}.tif").touch()
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

CREATE_METADATA = cli_touch_last_arg()

CMR_TO_STAC_ITEM = cli_touch_nth_arg(2)

CREATE_MANIFEST = cli_touch_nth_arg(2)

GRANULE_TO_GIBS = make_python_script("""
import sys
from pathlib import Path

working_dir, gibs_dir, base_name = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
gibs_subdir = gibs_dir / f"{base_name}_GIBS_ID"
gibs_subdir.mkdir(parents=True, exist_ok=True)
(gibs_subdir / f"{base_name}.xml").touch()
(gibs_subdir / f"{base_name}.tif").touch()
""")

VI_GENERATE_INDICES = make_python_script("""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-i")
parser.add_argument("-o", required=True)
parser.add_argument("-s", required=True)
args = parser.parse_args()

output_dir = Path(args.o)
output_dir.mkdir(parents=True, exist_ok=True)
(output_dir / f"{args.s}_NDVI.tif").touch()
""")

VI_GENERATE_METADATA = make_python_script("""
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-i")
parser.add_argument("-o")
parser.parse_args()
""")

VI_GENERATE_STAC_ITEMS = cli_touch_last_arg()


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
