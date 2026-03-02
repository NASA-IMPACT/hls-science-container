"""Sentinel-2 granule specific processing tasks

These mirror the "sentinel_granule.sh" script
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

from hls_nextgen_orchestration.base import (
    AssetBundle,
    MappedTask,
    TaskFailure,
)
from hls_nextgen_orchestration.common import Paths
from hls_nextgen_orchestration.utils import validate_command

from .assets import (
    CONFIG,
    EnvConfig,
    angle_hdf_asset,
    combined_sr_hdf_asset,
    detfoo_file_asset,
    espa_xml_asset,
    final_sr_hdf_asset,
    fmask_bin_asset,
    granule_dir_asset,
    lasrc_aerosol_qa_asset,
    mtd_msil1c_asset,
    mtd_tl_asset,
    quality_mask_applied_asset,
    safe_dir_asset,
    solar_valid_asset,
    split_hdf_parts_asset,
    trimmed_hdf_asset,
)

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


# ----- Tasks from sentinel.sh
@dataclass(frozen=True, kw_only=True)
class DownloadSentinelGranule(MappedTask):
    """
    Downloads the Sentinel-2 .zip granule and unzips it.
    Ports: aws s3 cp ... && unzip ...
    """

    requires = (CONFIG,)
    provides_factory = lambda granule_id: (safe_dir_asset(granule_id),)

    def __post_init__(self) -> None:
        validate_command("unzip")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        granule_dir = config.working_dir / self.granule_id

        zip_path = granule_dir / f"{self.granule_id}.zip"
        zip_path.parent.mkdir(exist_ok=True, parents=True)

        logger.info(f"Downloading s3://{config.input_bucket}/{self.granule_id}.zip")
        s3 = boto3.client("s3")
        s3.download_file(config.input_bucket, f"{self.granule_id}.zip", str(zip_path))

        logger.info(f"Unzipping {zip_path}")
        subprocess.run(
            ["unzip", "-q", str(zip_path), "-d", str(granule_dir)], check=True
        )

        safe_dir = granule_dir / f"{self.granule_id}.SAFE"
        if not safe_dir.exists():
            raise FileExistsError(f"Cannot find expected SAFE directory, {safe_dir}")

        return {safe_dir_asset(self.granule_id): safe_dir}


@dataclass(frozen=True, kw_only=True)
class LocalSentinelGranule(MappedTask):
    """Handles a pre-downloaded Sentinel-2 .zip granule.

    Useful for local testing or custom orchestration where data is already present.
    """

    local_granule_zip: Path

    requires = (CONFIG,)
    provides_factory = lambda granule_id: (safe_dir_asset(granule_id),)

    def __post_init__(self) -> None:
        validate_command("unzip")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        if not self.local_granule_zip.exists():
            raise TaskFailure(f"Local ZIP not found at {self.local_granule_zip}")

        config: EnvConfig = bundle[CONFIG]
        granule_dir = config.working_dir / self.granule_id

        zip_path = granule_dir / f"{self.granule_id}.zip"
        zip_path.parent.mkdir(exist_ok=True, parents=True)

        logger.info(f"Using local granule ZIP: {self.local_granule_zip}")
        shutil.copy(self.local_granule_zip, zip_path)

        logger.info(f"Unzipping {zip_path}")
        subprocess.run(
            ["unzip", "-q", str(zip_path), "-d", str(granule_dir)], check=True
        )

        safe_dir = granule_dir / f"{self.granule_id}.SAFE"
        if not safe_dir.exists():
            raise FileExistsError(f"Cannot find expected SAFE directory, {safe_dir}")

        return {safe_dir_asset(self.granule_id): safe_dir}


@dataclass(frozen=True, kw_only=True)
class GetGranuleDir(MappedTask):
    """Locates the granule directory within the unzipped SAFE directory.

    Ports: get_s2_granule_dir
    """

    requires_factory = lambda gid: (safe_dir_asset(gid),)
    provides_factory = lambda gid: (
        granule_dir_asset(gid),
        mtd_msil1c_asset(gid),
        mtd_tl_asset(gid),
    )

    def run(self, bundle: AssetBundle) -> AssetBundle:
        safe_dir = bundle[self.requires[0]]

        mtd_msil1c = safe_dir / "MTD_MSIL1C.xml"
        if not mtd_msil1c.exists():
            raise TaskFailure(f"No MTD_MSIL1C.xml file within {safe_dir}")

        # Find the first subdirectory - there should be just 1 granule
        granule_root = safe_dir / "GRANULE"
        subdirs = [d for d in granule_root.iterdir() if d.is_dir()]
        if not subdirs:
            raise TaskFailure(f"No subdirectory found in {granule_root}")
        granule_dir = subdirs[0]

        # Find SAFE metadata file inside granule directory
        #   1. newer SAFE `MTD_TL.xml`
        #   2. older format `S2[A|B|C]_OPER_MTD_*.xml`
        xmls = list(granule_dir.glob("*.xml"))
        if not xmls:
            raise TaskFailure(f"No MTD_TL.xml file within {granule_dir}")
        mtd_tl_xml = xmls[0]

        return {
            granule_dir_asset(self.granule_id): granule_dir,
            mtd_msil1c_asset(self.granule_id): mtd_msil1c,
            mtd_tl_asset(self.granule_id): mtd_tl_xml,
        }


@dataclass(frozen=True, kw_only=True)
class CheckSolarZenith(MappedTask):
    """Checks solar zenith angle validity.

    If the solar zenith angle is below the threshold this task will
    exit with exit code 3. This exit code is translated into an expected
    failure case by the job monitoring part of our workflow orchestration.
    """

    requires_factory = lambda gid: (mtd_tl_asset(gid),)
    provides_factory = lambda gid: (solar_valid_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("check_solar_zenith_sentinel")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        mtd_tl = bundle[self.requires[0]]

        logger.info("Checking solar zenith angle")
        result = subprocess.run(
            ["check_solar_zenith_sentinel", str(mtd_tl)],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout.strip() == "invalid":
            raise TaskFailure("Invalid solar zenith angle", exit_code=3)

        return {solar_valid_asset(self.granule_id): True}


@dataclass(frozen=True, kw_only=True)
class FindS2Footprint(MappedTask):
    """Locate the detector footprint for B06.

    Prior to baseline 04.00 the detector footprint was distributed in
    "GML" (Geography Markup Language) format. Since baseline 04.00 release
    the detector footprint ("detfoo") has been formatted as JPEG2000.

    This task handles finding the detector footprint file and potentially
    converting it into a format usable by the HLS production code.
    """

    requires_factory = lambda gid: (
        CONFIG,
        safe_dir_asset(gid),
        solar_valid_asset(gid),
    )
    provides_factory = lambda gid: (detfoo_file_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("gdal_translate")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        safe_dir = bundle[self.requires[1]]

        # Locate detector footprint for B06
        # Searching recursively in SAFE_DIR for MSK_DETFOO_B06.jp2 or .gml
        # This usually resides in the QI_DATA directory
        detfoo_candidates = list(safe_dir.rglob("MSK_DETFOO_B06.*"))

        # Filter for jp2 or gml
        detfoo06 = next(
            (f for f in detfoo_candidates if f.suffix in {".jp2", ".gml"}), None
        )
        if not detfoo06:
            raise TaskFailure("MSK_DETFOO_B06 (jp2/gml) not found in SAFE directory")

        # Convert JPEG2000 format to binary for HLS programs
        if detfoo06.suffix == ".jp2":
            detfoo06_bin = config.working_dir / self.granule_id / "MSK_DETFOO_B06.bin"
            logger.info(f"Converting {detfoo06} to {detfoo06_bin}")
            subprocess.run(
                ["gdal_translate", "-of", "ENVI", str(detfoo06), str(detfoo06_bin)],
                check=True,
            )
            detfoo06 = detfoo06_bin

        return {detfoo_file_asset(self.granule_id): detfoo06}


@dataclass(frozen=True, kw_only=True)
class ApplyS2QualityMask(MappedTask):
    """Applies ESA's pixel-level quality mask.

    Ports: apply_s2_quality_mask
    """

    requires_factory = lambda gid: (granule_dir_asset(gid), solar_valid_asset(gid))
    provides_factory = lambda gid: (quality_mask_applied_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("apply_s2_quality_mask")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        inner_dir = bundle[granule_dir_asset(self.granule_id)]
        logger.info(f"Applying quality mask in {inner_dir}")
        subprocess.run(["apply_s2_quality_mask", str(inner_dir)], check=True)
        return {quality_mask_applied_asset(self.granule_id): True}


@dataclass(frozen=True, kw_only=True)
class DeriveS2Angles(MappedTask):
    """Generates the angle HDF file.

    Ports: sentinel-derive-angle (with output args)
    """

    requires_factory = lambda gid: (
        CONFIG,
        mtd_tl_asset(gid),
        detfoo_file_asset(gid),
        quality_mask_applied_asset(gid),
    )
    provides_factory = lambda gid: (angle_hdf_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("sentinel-derive-angle")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        mtd_tl = bundle[mtd_tl_asset(self.granule_id)]
        detfoo06 = bundle[detfoo_file_asset(self.granule_id)]

        angle_output = config.working_dir / self.granule_id / "angle.hdf"
        detfoo_temp = config.working_dir / self.granule_id / "detfoo.hdf"

        logger.info("Running sentinel-derive-angle")
        subprocess.run(
            [
                "sentinel-derive-angle",
                str(mtd_tl),
                str(detfoo06),
                str(detfoo_temp),
                str(angle_output),
            ],
            check=True,
        )

        # The detfoo output is an unnecessary legacy output
        if detfoo_temp.exists():
            detfoo_temp.unlink()

        return {angle_hdf_asset(self.granule_id): angle_output}


@dataclass(frozen=True, kw_only=True)
class RunFmask(MappedTask):
    """Runs Fmask on the Sentinel granule.

    Ports: run_Fmask.sh and gdal_translate
    """

    requires_factory = lambda gid: (
        CONFIG,
        granule_dir_asset(gid),
        quality_mask_applied_asset(gid),
        mtd_msil1c_asset(gid),
    )
    provides_factory = lambda gid: (fmask_bin_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("check_sentinel_clouds")
        validate_command("run_Fmask.sh")
        validate_command("parse_fmask")
        validate_command("gdal_translate")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        safe_inner_dir = bundle[granule_dir_asset(self.granule_id)]
        mtd_msil1c = bundle[mtd_msil1c_asset(self.granule_id)]

        logger.info(f"Running Fmask in {safe_inner_dir}")
        fmask_log = safe_inner_dir / "fmask_out.txt"
        with open(fmask_log, "w") as outfile:
            subprocess.run(
                ["run_Fmask.sh"], cwd=safe_inner_dir, stdout=outfile, check=True
            )

        invalid = self.check_invalid_cloud_cover(mtd_msil1c, fmask_log)
        if invalid:
            raise TaskFailure("Fmask reports no clear pixels. Exiting now", exit_code=4)

        # Find the generated TIF - it should be inside the "granule dir" like,
        # {WORKING_DIR}/{GRANULE_ID}.SAFE/GRANULE/{GRANULE_ID}/FMASK_DATA/{GRANULE_ID}_Fmask4.tif
        # This is complicated, so it's easier to recursively glob for it.
        fmask_tif = next(safe_inner_dir.rglob("*_Fmask4.tif"))
        fmask_bin = config.working_dir / self.granule_id / "fmask.bin"

        logger.info(f"Converting {fmask_tif} to {fmask_bin}")
        subprocess.run(
            ["gdal_translate", "-of", "ENVI", str(fmask_tif), str(fmask_bin)],
            check=True,
        )

        return {fmask_bin_asset(self.granule_id): fmask_bin}

    def check_invalid_cloud_cover(self, mtd_msil1c: Path, fmask_log: Path) -> bool:
        """Check if the cloud cover is invalid

        This check requires that both:
        1. Fmask shows less than 2% clear
        2. Sentinel-2 L1C metadata shows greater than 95% cloud cover.

        Returns
        -------
        bool
            True if invalid
        """
        # Check Sentinel-2 L1C metadata
        logger.info("Checking Sentinel-2 metadata cloud cover...")
        result = subprocess.run(
            ["check_sentinel_clouds", str(mtd_msil1c)],
            check=True,
            text=True,
            capture_output=True,
        )
        l1c_report = result.stdout.strip()
        l1c_invalid = l1c_report == "invalid"

        # Read 2nd to last line of Fmask output
        with fmask_log.open() as src:
            fmask_report = src.readlines()[-2]

        result = subprocess.run(
            ["parse_fmask", fmask_report],
            check=True,
            text=True,
            capture_output=True,
        )
        fmask_result = result.stdout.strip()
        fmask_invalid = fmask_result == "invalid"

        return l1c_invalid and fmask_invalid


@dataclass(frozen=True, kw_only=True)
class PrepareEspaInput(MappedTask):
    """Unpackages S2 and converts to ESPA format.

    This task unzips the Sentinel-2 SAFE zip archive in an
    ESPA compatible manner, which redefines the locations
    of the MTD_TL and MTD_MSIL1C metadata.

    Ports: unpackage_s2.py, convert_sentinel_to_espa
    """

    requires_factory = lambda gid: (CONFIG, safe_dir_asset(gid), fmask_bin_asset(gid))
    provides_factory = lambda gid: (
        mtd_tl_asset(gid),
        mtd_msil1c_asset(gid),
        espa_xml_asset(gid),
    )

    def __post_init__(self) -> None:
        validate_command("unpackage_s2.py")
        validate_command("convert_sentinel_to_espa")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        safe_dir = bundle[safe_dir_asset(self.granule_id)]

        granule_dir = config.working_dir / self.granule_id

        # Delete the original SAFE zip to save disk space
        (config.working_dir / self.granule_id / f"{self.granule_id}.zip").unlink()

        # Script re-zips the masked directory. Skipping zip overhead for local processing
        # if unpackage_s2.py accepts directory, but script passes zip.
        # Assuming unpackage_s2.py needs a zip:
        masked_zip = granule_dir / "masked.zip"
        shutil.make_archive(
            base_name=str(masked_zip.with_suffix("")),
            format="zip",
            root_dir=safe_dir.parent,
            # ZIP needs to contain the SAFE zip as a folder so `unpackage_s2.py`
            # can figure out the product ID
            base_dir=safe_dir.name,
        )

        subprocess.run(
            ["unpackage_s2.py", "-i", str(masked_zip), "-o", str(granule_dir)],
            check=True,
        )
        subprocess.run(["convert_sentinel_to_espa"], cwd=safe_dir, check=True)

        # FIXME: if not debug, delete JP2 files to save space

        # Find ESPA XML
        xmls = list(safe_dir.glob("*.xml"))
        ignore = {"MTD_TL.xml", "MTD_MSIL1C.xml"}
        espa_xml = next((f for f in xmls if f.name not in ignore), None)

        if not espa_xml:
            raise TaskFailure("ESPA XML not found")

        # Relocate MTD metadata files post-unzip
        mtd_tl = list(granule_dir.rglob("MTD_TL.xml"))[0]
        mtd_msil1c = list(granule_dir.rglob("MTD_MSIL1C.xml"))[0]

        return {
            mtd_tl_asset(self.granule_id): mtd_tl,
            mtd_msil1c_asset(self.granule_id): mtd_msil1c,
            espa_xml_asset(self.granule_id): espa_xml,
        }


@dataclass(frozen=True, kw_only=True)
class RunLaSRC(MappedTask):
    """Runs LaSRC for Sentinel."""

    requires_factory = lambda gid: (espa_xml_asset(gid),)
    provides_factory = lambda gid: (lasrc_aerosol_qa_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("do_lasrc_sentinel.py")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        espa_xml = bundle[espa_xml_asset(self.granule_id)]

        # The script runs in the directory of the XML
        output_dir = espa_xml.parent

        subprocess.run(
            ["do_lasrc_sentinel.py", "--xml", str(espa_xml)], cwd=output_dir, check=True
        )

        espa_id = espa_xml.stem
        aerosol_qa = output_dir / f"{espa_id}_sr_aerosol_qa.img"

        if not aerosol_qa.exists():
            raise TaskFailure(
                f"Cannot find the LaSRC aerosol QA output (expected {aerosol_qa})"
            )

        return {lasrc_aerosol_qa_asset(self.granule_id): aerosol_qa}


@dataclass(frozen=True, kw_only=True)
class ProcessHdfParts(MappedTask):
    """Splits XMLs and converts to HDF parts (One/Two).

    Ports: create_sr_hdf_xml, convert_espa_to_hdf
    """

    requires_factory = lambda gid: (
        CONFIG,
        espa_xml_asset(gid),
        lasrc_aerosol_qa_asset(gid),
    )
    provides_factory = lambda gid: (split_hdf_parts_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("create_sr_hdf_xml")
        validate_command("convert_sentinel_to_espa")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        espa_xml = bundle[espa_xml_asset(self.granule_id)]
        espa_id = espa_xml.stem

        # NOTE: the programs below do NOT work with fully qualified file
        #       paths, only with relative paths.
        os.chdir(espa_xml.parent)

        parts = Paths()
        for part, suffix in [("one", "1"), ("two", "2")]:
            hls_xml = espa_xml.parent / f"{espa_id}_{suffix}_hls.xml"
            out_hdf = espa_xml.parent / f"{espa_id}_sr_{suffix}.hdf"

            # create_sr_hdf_xml "$espa_xml" "$hls_espa_one_xml" one
            subprocess.run(
                ["create_sr_hdf_xml", espa_xml.name, hls_xml.name, part], check=True
            )

            # convert_espa_to_hdf --xml="$hls_espa_one_xml" --hdf="$sr_hdf_one"
            subprocess.run(
                [
                    "convert_espa_to_hdf",
                    f"--xml={hls_xml.name}",
                    f"--hdf={out_hdf.name}",
                ],
                check=True,
            )
            parts.append(out_hdf)

        return {split_hdf_parts_asset(self.granule_id): parts}


@dataclass(frozen=True, kw_only=True)
class CombineS2Hdf(MappedTask):
    """Combine split hdf files and resample 10M SR bands back to 20M and 60M.

    Ports: sentinel-twohdf2one
    """

    requires_factory = lambda gid: (
        CONFIG,
        espa_xml_asset(gid),
        split_hdf_parts_asset(gid),
    )
    provides_factory = lambda gid: (combined_sr_hdf_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("sentinel-twohdf2one")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        espa_xml = bundle[espa_xml_asset(self.granule_id)]
        parts = bundle[split_hdf_parts_asset(self.granule_id)]

        espa_id = espa_xml.stem
        combined_hdf = espa_xml.parent / f"{espa_id}_sr_combined.hdf"

        os.chdir(espa_xml.parent)

        # sentinel-twohdf2one "$sr_hdf_one" "$sr_hdf_two" MTD_MSIL1C.xml MTD_TL.xml "$ACCODE" "$hls_sr_combined_hdf"
        subprocess.run(
            [
                "sentinel-twohdf2one",
                parts[0].name,
                parts[1].name,
                "MTD_MSIL1C.xml",
                "MTD_TL.xml",
                config.ac_code,
                combined_hdf.name,
            ],
            check=True,
        )

        return {combined_sr_hdf_asset(self.granule_id): combined_hdf}


@dataclass(frozen=True, kw_only=True)
class AddS2FmaskSds(MappedTask):
    """
    Adds Fmask SDS to HDF.
    Ports: sentinel-add-fmask-sds
    """

    requires_factory = lambda gid: (
        CONFIG,
        mtd_tl_asset(gid),
        mtd_msil1c_asset(gid),
        combined_sr_hdf_asset(gid),
        lasrc_aerosol_qa_asset(gid),
        fmask_bin_asset(gid),
    )
    provides_factory = lambda gid: (final_sr_hdf_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("sentinel-add-fmask-sds")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        combined_hdf = bundle[combined_sr_hdf_asset(self.granule_id)]
        aerosol_qa = bundle[lasrc_aerosol_qa_asset(self.granule_id)]
        fmask_bin = bundle[fmask_bin_asset(self.granule_id)]
        mtd_tl = bundle[mtd_tl_asset(self.granule_id)]
        mtd_msil1c = bundle[mtd_msil1c_asset(self.granule_id)]

        final_sr = config.working_dir / self.granule_id / "sr.hdf"

        # sentinel-add-fmask-sds "$hls_sr_combined_hdf" "$fmaskbin" \
        #   "$aerosol_qa" MTD_MSIL1C.xml MTD_TL.xml \
        #   "$ACCODE" "$hls_sr_output_hdf"
        subprocess.run(
            [
                "sentinel-add-fmask-sds",
                str(combined_hdf),
                str(fmask_bin),
                str(aerosol_qa),
                str(mtd_msil1c),
                str(mtd_tl),
                config.ac_code,
                str(final_sr),
            ],
            check=True,
        )

        return {final_sr_hdf_asset(self.granule_id): final_sr}


@dataclass(frozen=True, kw_only=True)
class TrimS2Hdf(MappedTask):
    """Trims edge pixels.

    Ports: sentinel-trim
    """

    requires_factory = lambda gid: (final_sr_hdf_asset(gid),)
    provides_factory = lambda gid: (trimmed_hdf_asset(gid),)

    def __post_init__(self) -> None:
        validate_command("sentinel-trim")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        hdf = bundle[final_sr_hdf_asset(self.granule_id)]
        subprocess.run(["sentinel-trim", str(hdf)], check=True)
        # FIXME: cleanup .SAFE directory if not debug mode
        return {trimmed_hdf_asset(self.granule_id): hdf}
