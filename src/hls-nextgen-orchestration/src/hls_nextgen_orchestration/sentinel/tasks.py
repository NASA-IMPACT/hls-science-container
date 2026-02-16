from __future__ import annotations

import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

from hls_nextgen_orchestration.base import (
    Asset,
    AssetBundle,
    DataSource,
    Task,
    TaskFailure,
)
from hls_nextgen_orchestration.common.commands import run_hdf_to_cog
from hls_nextgen_orchestration.granules import HlsGranule, Sentinel2Granule
from hls_nextgen_orchestration.utils import validate_command
from hls_nextgen_orchestration.version import HLS_VERSION, HlsVersion

from .assets import (
    ANGLE_HDF,
    CMR_XML,
    COGS_CREATED,
    COMBINED_SR_HDF,
    CONFIG,
    DETFOO_FILE,
    ESPA_XML,
    FINAL_OUTPUT_HDF,
    FINAL_SR_HDF,
    FMASK_BIN,
    GIBS_DIR,
    GIBS_MANIFEST_FILES,
    GRANULE_MTD_TL,
    LASRC_AEROSOL_QA,
    NBAR_HDF,
    NBAR_INPUT_HDF,
    OUTPUT_BASE_NAME,
    QUALITY_MASK_APPLIED,
    RENAMED_ANGLE_HDF,
    RENAMED_HDF,
    RESAMPLED_HDF,
    SAFE_DIR,
    SAFE_PRODUCT_GRANULE_DIR,
    SOLAR_VALID,
    SPLIT_HDF_PARTS,
    SR_MANIFEST_FILE,
    STAC_JSON,
    THUMBNAIL_FILE,
    TRIMMED_HDF,
    UPLOAD_COMPLETE,
    VI_DIR,
    VI_MANIFEST_FILE,
    EnvConfig,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


logger = logging.getLogger(__name__)


# ----- Naming conventions
def sentinel_to_hls_granule(sentinel_granule_id: str) -> str:
    """Get the HLS product output name for a Sentinel-2 granule ID"""
    s2_granule = Sentinel2Granule.from_str(sentinel_granule_id)
    hls_granule = HlsGranule(
        product="HLS",
        sensor="S30",
        tile_id=s2_granule.tile_id,
        acquisition_time=s2_granule.acquisition_time,
    )
    return hls_granule.to_str()


def sentinel_to_nbar_hdf_filename(
    granule: Sentinel2Granule,
    hls_version: HlsVersion = HLS_VERSION,
) -> str:
    """Filename for the intermediate NBAR HDF output"""
    # nbar_name="HLS.S30.${granulecomponents[5]}.${year}${day_of_year}.${hms}.${hlsversion}"
    # nbar_input="${workingdir}/${nbar_name}.hdf"
    name = ".".join(
        [
            "HLS",
            "S30",
            granule.acquisition_time.strftime("%Y%j"),
            granule.acquisition_time.strftime("%H%M%S"),
            hls_version.to_str(),
        ]
    )
    return f"{name}.hdf"


# ----- Environment variable and config setup
@dataclass(frozen=True, kw_only=True)
class EnvSource(DataSource):
    """
    Reads Sentinel-2 specific environment variables to configure the processing job.
    Matches variables found in sentinel.sh and sentinel_granule.sh.
    """

    # FIXME: ughhh twin graule

    provides = (CONFIG,)

    scratch_dir: Path = field(
        default_factory=lambda: Path(os.getenv("SCRATCH_DIR", "/var/scratch"))
    )
    working_dir: Path | None = None
    granule_dir: Path | None = None

    def fetch(self) -> dict[Asset[EnvConfig], EnvConfig]:
        job_id = os.getenv("AWS_BATCH_JOB_ID", "local_job")
        granule = os.environ["GRANULE"]

        working_dir = self.working_dir or self.scratch_dir / job_id
        granule_dir = self.granule_dir or working_dir / granule

        config = EnvConfig(
            job_id=job_id,
            granule=granule,
            input_bucket=os.environ["INPUT_BUCKET"],
            output_bucket=os.environ["OUTPUT_BUCKET"],
            gibs_bucket=os.environ["GIBS_OUTPUT_BUCKET"],
            prefix=os.environ["PREFIX"],
            ac_code=os.environ["ACCODE"],
            working_dir=working_dir,
            granule_dir=granule_dir,
            debug_bucket=os.getenv("DEBUG_BUCKET"),
            replace_existing=os.getenv("REPLACE_EXISTING", "false").lower() == "true",
        )

        # Ensure directory exists
        config.granule_dir.mkdir(parents=True, exist_ok=True)

        return {CONFIG: config}


# ----- Tasks from sentinel.sh
@dataclass(frozen=True, kw_only=True)
class DownloadSentinelGranule(Task):
    """
    Downloads the Sentinel-2 .zip granule and unzips it.
    Ports: aws s3 cp ... && unzip ...
    """

    requires = (CONFIG,)
    provides = (SAFE_DIR,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        granule_id = config.granule
        zip_path = config.granule_dir / f"{granule_id}.zip"

        logger.info(f"Downloading s3://{config.input_bucket}/{granule_id}.zip")
        s3 = boto3.client("s3")
        s3.download_file(config.input_bucket, f"{granule_id}.zip", str(zip_path))

        logger.info(f"Unzipping {zip_path}")
        subprocess.run(
            ["unzip", "-q", str(zip_path), "-d", str(config.granule_dir)], check=True
        )

        safe_dir = config.granule_dir / f"{granule_id}.SAFE"
        return {SAFE_DIR: safe_dir}


@dataclass(frozen=True, kw_only=True)
class LocalSentinelGranule(Task):
    """Handles a pre-downloaded Sentinel-2 .zip granule.

    Useful for local testing or custom orchestration where data is already present.
    """

    requires = (CONFIG,)
    provides = (SAFE_DIR,)

    local_granule_zip: Path

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        granule_id = config.granule
        dest_zip = config.granule_dir / f"{granule_id}.zip"

        if not self.local_granule_zip.exists():
            raise TaskFailure(f"Local ZIP not found at {self.local_granule_zip}")

        logger.info(f"Using local granule ZIP: {self.local_granule_zip}")
        shutil.copy(self.local_granule_zip, dest_zip)

        logger.info(f"Unzipping {dest_zip}")
        subprocess.run(
            ["unzip", "-q", str(dest_zip), "-d", str(config.granule_dir)], check=True
        )

        safe_dir = config.granule_dir / f"{granule_id}.SAFE"
        return {SAFE_DIR: safe_dir}


@dataclass(frozen=True, kw_only=True)
class GetGranuleDir(Task):
    """Locates the granule directory within the unzipped SAFE directory.

    Ports: get_s2_granule_dir
    """

    requires = (SAFE_DIR,)
    provides = (SAFE_PRODUCT_GRANULE_DIR, GRANULE_MTD_TL)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        safe_dir = bundle[SAFE_DIR]
        granule_root = safe_dir / "GRANULE"

        # Find the first subdirectory - there should be just 1 granule
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

        return {SAFE_PRODUCT_GRANULE_DIR: granule_dir, GRANULE_MTD_TL: mtd_tl_xml}


@dataclass(frozen=True, kw_only=True)
class CheckSolarZenith(Task):
    """Checks solar zenith angle validity.

    If the solar zenith angle is below the threshold this task will
    exit with exit code 3. This exit code is translated into an expected
    failure case by the job monitoring part of our workflow orchestration.
    """

    requires = (GRANULE_MTD_TL,)
    provides = (SOLAR_VALID,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        mtd_tl = bundle[GRANULE_MTD_TL]

        logger.info("Checking solar zenith angle")
        result = subprocess.run(
            ["check_solar_zenith_sentinel", str(mtd_tl)],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout.strip() == "invalid":
            raise TaskFailure("Invalid solar zenith angle", exit_code=3)

        return {SOLAR_VALID: True}


@dataclass(frozen=True, kw_only=True)
class FindS2Footprint(Task):
    """Locate the detector footprint for B06.

    Prior to baseline 04.00 the detector footprint was distributed in
    "GML" (Geography Markup Language) format. Since baseline 04.00 release
    the detector footprint ("detfoo") has been formatted as JPEG2000.

    This task handles finding the detector footprint file and potentially
    converting it into a format usable by the HLS production code.
    """

    requires = (SAFE_DIR, CONFIG)
    provides = (DETFOO_FILE,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        safe_dir = bundle[SAFE_DIR]
        config = bundle[CONFIG]

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
            detfoo06_bin = config.granule_dir / "MSK_DETFOO_B06.bin"
            logger.info(f"Converting {detfoo06} to {detfoo06_bin}")
            subprocess.run(
                ["gdal_translate", "-of", "ENVI", str(detfoo06), str(detfoo06_bin)],
                check=True,
            )
            detfoo06 = detfoo06_bin

        return {DETFOO_FILE: detfoo06}


@dataclass(frozen=True, kw_only=True)
class ApplyS2QualityMask(Task):
    """Applies ESA's pixel-level quality mask.

    Ports: apply_s2_quality_mask
    """

    requires = (SAFE_PRODUCT_GRANULE_DIR,)
    provides = (QUALITY_MASK_APPLIED,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        inner_dir = bundle[SAFE_PRODUCT_GRANULE_DIR]
        logger.info(f"Applying quality mask in {inner_dir}")
        subprocess.run(["apply_s2_quality_mask", str(inner_dir)], check=True)
        return {QUALITY_MASK_APPLIED: True}


@dataclass(frozen=True, kw_only=True)
class DeriveS2Angles(Task):
    """Generates the angle HDF file.

    Ports: sentinel-derive-angle (with output args)
    """

    requires = (
        GRANULE_MTD_TL,
        CONFIG,
        SOLAR_VALID,
        DETFOO_FILE,
        QUALITY_MASK_APPLIED,
    )
    provides = (ANGLE_HDF,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        mtd_tl = bundle[GRANULE_MTD_TL]
        config = bundle[CONFIG]
        detfoo06 = bundle[DETFOO_FILE]
        angle_output = config.granule_dir / "angle.hdf"

        detfoo_temp = config.granule_dir / "detfoo.hdf"
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

        return {ANGLE_HDF: angle_output}


# FIXME: refactor this to use Landsat version
@dataclass(frozen=True, kw_only=True)
class RunFmask(Task):
    """Runs Fmask on the Sentinel granule.

    Ports: run_Fmask.sh and gdal_translate
    """

    requires = (SAFE_PRODUCT_GRANULE_DIR, CONFIG)
    provides = (FMASK_BIN,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        safe_inner_dir = bundle[SAFE_PRODUCT_GRANULE_DIR]
        config = bundle[CONFIG]
        fmask_bin = config.granule_dir / "fmask.bin"

        # Script logic: cd to inner dir, run Fmask, capture output filename
        logger.info(f"Running Fmask in {safe_inner_dir}")

        # In python we can pass cwd to subprocess
        with open(safe_inner_dir / "fmask_out.txt", "w") as outfile:
            subprocess.run(
                ["run_Fmask.sh"], cwd=safe_inner_dir, stdout=outfile, check=True
            )

        # Find the generated TIF (script parses fmask_out.txt, we can just glob)
        fmask_tif = next(safe_inner_dir.glob("*_Fmask4.tif"))

        logger.info(f"Converting {fmask_tif} to {fmask_bin}")
        subprocess.run(
            ["gdal_translate", "-of", "ENVI", str(fmask_tif), str(fmask_bin)],
            check=True,
        )

        return {FMASK_BIN: fmask_bin}


@dataclass(frozen=True, kw_only=True)
class PrepareEspaInput(Task):
    """Unpackages S2 and converts to ESPA format.

    Ports: unpackage_s2.py, convert_sentinel_to_espa
    """

    requires = (SAFE_DIR, CONFIG)
    provides = (ESPA_XML,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        safe_dir = bundle[SAFE_DIR]
        config = bundle[CONFIG]

        # Script re-zips the masked directory. Skipping zip overhead for local processing
        # if unpackage_s2.py accepts directory, but script passes zip.
        # Assuming unpackage_s2.py needs a zip:
        masked_zip = config.granule_dir / "masked.zip"
        shutil.make_archive(str(masked_zip.with_suffix("")), "zip", safe_dir)

        subprocess.run(
            ["unpackage_s2.py", "-i", str(masked_zip), "-o", str(config.granule_dir)],
            check=True,
        )
        # FIXME: delete the original SAFE zip

        subprocess.run(["convert_sentinel_to_espa"], cwd=safe_dir, check=True)

        # FIXME: if not debug, delete JP2 files to save space

        # Find ESPA XML
        xmls = list(safe_dir.glob("*.xml"))
        ignore = {"MTD_TL.xml", "MTD_MSIL1C.xml"}
        espa_xml = next((f for f in xmls if f.name not in ignore), None)

        if not espa_xml:
            raise TaskFailure("ESPA XML not found")

        return {ESPA_XML: espa_xml}


@dataclass(frozen=True, kw_only=True)
class RunLaSRC(Task):
    """Runs LaSRC for Sentinel."""

    requires = (ESPA_XML,)
    provides = (LASRC_AEROSOL_QA,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        espa_xml = bundle[ESPA_XML]
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

        return {LASRC_AEROSOL_QA: output_dir}


@dataclass(frozen=True, kw_only=True)
class ProcessHdfParts(Task):
    """
    Splits XMLs and converts to HDF parts (One/Two).
    Ports: create_sr_hdf_xml, convert_espa_to_hdf
    """

    requires = (ESPA_XML, CONFIG, LASRC_AEROSOL_QA)
    provides = (SPLIT_HDF_PARTS,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        espa_xml = bundle[ESPA_XML]
        config = bundle[CONFIG]
        espa_id = espa_xml.stem

        parts = []
        for part, suffix in [("one", "1"), ("two", "2")]:
            hls_xml = config.granule_dir / f"{espa_id}_{suffix}_hls.xml"
            out_hdf = config.granule_dir / f"{espa_id}_sr_{suffix}.hdf"

            # create_sr_hdf_xml "$espa_xml" "$hls_espa_one_xml" one
            subprocess.run(
                ["create_sr_hdf_xml", str(espa_xml), str(hls_xml), part], check=True
            )

            # convert_espa_to_hdf --xml="$hls_espa_one_xml" --hdf="$sr_hdf_one"
            subprocess.run(
                ["convert_espa_to_hdf", f"--xml={hls_xml}", f"--hdf={out_hdf}"],
                check=True,
            )
            parts.append(out_hdf)

        return {SPLIT_HDF_PARTS: parts}


@dataclass(frozen=True, kw_only=True)
class CombineS2Hdf(Task):
    """Combine split hdf files and resample 10M SR bands back to 20M and 60M.

    Ports: sentinel-twohdf2one
    """

    requires = (ESPA_XML, SPLIT_HDF_PARTS, CONFIG)
    provides = (COMBINED_SR_HDF,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        espa_xml = bundle[ESPA_XML]
        parts = bundle[SPLIT_HDF_PARTS]
        config = bundle[CONFIG]

        espa_id = espa_xml.stem
        combined_hdf = config.granule_dir / f"{espa_id}_sr_combined.hdf"

        # sentinel-twohdf2one "$sr_hdf_one" "$sr_hdf_two" MTD_MSIL1C.xml MTD_TL.xml "$ACCODE" "$hls_sr_combined_hdf"
        subprocess.run(
            [
                "sentinel-twohdf2one",
                parts[0],
                parts[1],
                "MTD_MSIL1C.xml",
                "MTD_TL.xml",
                config.ac_code,
                str(combined_hdf),
            ],
            check=True,
        )

        return {COMBINED_SR_HDF: combined_hdf}


@dataclass(frozen=True, kw_only=True)
class AddS2FmaskSds(Task):
    """
    Adds Fmask SDS to HDF.
    Ports: sentinel-add-fmask-sds
    """

    requires = (COMBINED_SR_HDF, LASRC_AEROSOL_QA, FMASK_BIN, CONFIG)
    provides = (FINAL_SR_HDF,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        combined_hdf = bundle[COMBINED_SR_HDF]
        aerosol_qa = bundle[LASRC_AEROSOL_QA]
        fmask_bin = bundle[FMASK_BIN]
        config = bundle[CONFIG]

        final_sr = config.granule_dir / "sr.hdf"

        # sentinel-add-fmask-sds "$hls_sr_combined_hdf" "$fmaskbin" \
        #   "$aerosol_qa" MTD_MSIL1C.xml MTD_TL.xml \
        #   "$ACCODE" "$hls_sr_output_hdf"
        subprocess.run(
            [
                "sentinel-add-fmask-sds",
                str(combined_hdf),
                str(fmask_bin),
                str(aerosol_qa),
                "MTD_MSIL1C.xml",
                "MTD_TL.xml",
                config.ac_code,
                str(final_sr),
            ],
            check=True,
        )
        return {FINAL_SR_HDF: final_sr}


@dataclass(frozen=True, kw_only=True)
class TrimS2Hdf(Task):
    """
    Trims edge pixels.
    Ports: sentinel-trim
    """

    requires = (FINAL_SR_HDF,)
    provides = (TRIMMED_HDF,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        hdf = bundle[FINAL_SR_HDF]
        subprocess.run(["sentinel-trim", str(hdf)], check=True)
        return {TRIMMED_HDF: hdf}


# ----- Tasks from sentinel.sh
@dataclass(frozen=True, kw_only=True)
class Resample30m(Task):
    """Resamples S2 to 30m.

    Ports: sentinel-create-s2at30m

    Notes
    -----
    When not in debug mode, the `RESAMPLE_HDF` Asset and `NBAR_INPUT_HDF` will be
    the same file. We copy the `RESAMPLED_HDF` file to `NBAR_INPUT_HDF` for debug
    mode because subsequent steps modify `NBAR_INPUT_HDF` in-place.
    """

    requires = (TRIMMED_HDF, CONFIG)
    provides = (RESAMPLED_HDF, NBAR_INPUT_HDF)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        input_hdf = bundle[TRIMMED_HDF]
        config: EnvConfig = bundle[CONFIG]
        resampled_output = config.working_dir / "resample30m.hdf"
        resampled_output_hdr = resampled_output.with_suffix(".hdf.hdr")

        subprocess.run(
            ["sentinel-create-s2at30m", str(input_hdf), str(resampled_output)],
            check=True,
        )

        # The next steps modify the HDF file IN PLACE, so copy to
        # maintain intermediate 30m version in debug mode.
        nbar_input_fname = sentinel_to_nbar_hdf_filename(config.sentinel_granule)
        nbar_input = config.working_dir / nbar_input_fname
        nbar_input_hdr = nbar_input.with_suffix(".hdf.hdr")

        if config.debug_bucket:
            logger.info(f"Copying {resampled_output} to {nbar_input} for debugging...")
            shutil.copy(resampled_output, nbar_input)
            shutil.copy(resampled_output_hdr, nbar_input_hdr)
        else:
            logger.info(f"Renaming {resampled_output} to {nbar_input}...")
            resampled_output.rename(nbar_input)
            resampled_output_hdr.rename(nbar_input_hdr)

        return {
            RESAMPLED_HDF: resampled_output,
            NBAR_INPUT_HDF: nbar_input,
        }


@dataclass(frozen=True, kw_only=True)
class DeriveNbar(Task):
    """Runs NBAR correction on SR HDF file

    Note: This step modifies the input IN PLACE.
    """

    # FIXME: do we need to pass along the `cfactor.hdf`?

    requires = (NBAR_INPUT_HDF, ANGLE_HDF, CONFIG)
    provides = (NBAR_HDF,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        nbar_hdf = bundle[NBAR_INPUT_HDF]
        angle = bundle[ANGLE_HDF]
        config = bundle[CONFIG]

        # cfactor.hdf is created as part of NBAR correction
        cfactor = config.granule_dir / "cfactor.hdf"

        subprocess.run(
            ["sentinel-derive-nbar", str(nbar_hdf), str(angle), str(cfactor)],
            check=True,
        )
        return {NBAR_HDF: nbar_hdf}


@dataclass(frozen=True, kw_only=True)
class BandpassCorrection(Task):
    """Applies L8-like bandpass correction to a HDF.

    Note: This step modifies the input IN PLACE.
    """

    requires = (NBAR_HDF, CONFIG)
    provides = (FINAL_OUTPUT_HDF,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        nbar_hdf = bundle[NBAR_HDF]
        granule = bundle[CONFIG].sentinel_granule

        param_file = f"bandpass_parameter.{granule.mission}.txt"
        subprocess.run(["sentinel-l8-like", param_file, str(nbar_hdf)], check=True)

        return {FINAL_OUTPUT_HDF: nbar_hdf}


@dataclass(frozen=True, kw_only=True)
class RenameS2Outputs(Task):
    """
    Renames the final HDF and Angle HDF to the standard HLS S30 naming convention.
    Mirrors `set_output_names` and `mv` commands in sentinel.sh.
    """

    requires = (CONFIG, FINAL_OUTPUT_HDF, ANGLE_HDF)
    provides = (OUTPUT_BASE_NAME, RENAMED_HDF, RENAMED_ANGLE_HDF)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        hdf_path = bundle[FINAL_OUTPUT_HDF]
        angle_path = bundle[ANGLE_HDF]

        base_name = sentinel_to_hls_granule(config.granule)
        renamed_hdf = config.granule_dir / f"{base_name}.hdf"
        renamed_angle = config.granule_dir / f"{base_name}.ANGLE.hdf"

        logger.info(f"Renaming outputs to {base_name}")

        # Rename Product
        if hdf_path != renamed_hdf:
            shutil.move(str(hdf_path), str(renamed_hdf))
            # Handle HDR if exists (ENVI header)
            hdr_src = hdf_path.with_suffix(".hdf.hdr")
            hdr_dst = renamed_hdf.with_suffix(".hdf.hdr")
            if hdr_src.exists():
                shutil.move(str(hdr_src), str(hdr_dst))

        # Rename Angle
        if angle_path != renamed_angle:
            shutil.move(str(angle_path), str(renamed_angle))
            # Angle might not have HDR depending on step, but check anyway
            angle_hdr_src = angle_path.with_suffix(".hdf.hdr")
            angle_hdr_dst = renamed_angle.with_suffix(".hdf.hdr")
            if angle_hdr_src.exists():
                shutil.move(str(angle_hdr_src), str(angle_hdr_dst))

        return {
            OUTPUT_BASE_NAME: base_name,
            RENAMED_HDF: renamed_hdf,
            RENAMED_ANGLE_HDF: renamed_angle,
        }


@dataclass(frozen=True, kw_only=True)
class S2ConvertToCogs(Task):
    """
    Converts S2 HDF outputs to COG format.
    Ports: hdf_to_cog
    """

    requires = (CONFIG, RENAMED_HDF, RENAMED_ANGLE_HDF)
    provides = (COGS_CREATED,)

    def __post_init__(self) -> None:
        validate_command("hdf_to_cog")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        hdf = bundle[RENAMED_HDF]
        angle = bundle[RENAMED_ANGLE_HDF]

        logger.info("Converting Product to COGs")
        # Uses the command helper
        run_hdf_to_cog(input_file=hdf, output_dir=config.working_dir, product="S30")

        logger.info("Converting Angle to COGs")
        # Uses the command helper
        run_hdf_to_cog(
            input_file=angle, output_dir=config.working_dir, product="S30_ANGLES"
        )
        return {COGS_CREATED: True}


@dataclass(frozen=True, kw_only=True)
class S2CreateThumbnail(Task):
    """
    Creates thumbnail for S30.
    Ports: create_thumbnail
    """

    requires = (CONFIG, OUTPUT_BASE_NAME, COGS_CREATED)
    provides = (THUMBNAIL_FILE,)

    def __post_init__(self) -> None:
        validate_command("create_thumbnail")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        base_name = bundle[OUTPUT_BASE_NAME]
        thumb_path = config.working_dir / f"{base_name}.jpg"

        logger.info("Creating Thumbnail")
        subprocess.run(
            [
                "create_thumbnail",
                "-i",
                str(config.working_dir),
                "-o",
                str(thumb_path),
                "-s",
                "S30",
            ],
            check=True,
        )
        return {THUMBNAIL_FILE: thumb_path}


@dataclass(frozen=True, kw_only=True)
class S2CreateMetadata(Task):
    """
    Generates CMR XML and STAC JSON.
    Ports: create_metadata, cmr_to_stac_item
    """

    requires = (CONFIG, RENAMED_HDF, OUTPUT_BASE_NAME, COGS_CREATED)
    provides = (CMR_XML, STAC_JSON)

    def __post_init__(self) -> None:
        validate_command("create_metadata")
        validate_command("cmr_to_stac_item")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        hdf = bundle[RENAMED_HDF]
        base_name = bundle[OUTPUT_BASE_NAME]

        cmr_xml = config.working_dir / f"{base_name}.cmr.xml"
        stac_json = config.working_dir / f"{base_name}_stac.json"

        logger.info("Creating Metadata")
        subprocess.run(
            ["create_metadata", str(hdf), "--save", str(cmr_xml)], check=True
        )

        logger.info("Creating STAC Item")
        subprocess.run(
            [
                "cmr_to_stac_item",
                str(cmr_xml),
                str(stac_json),
                "data.lpdaac.earthdatacloud.nasa.gov",
                "020",
            ],
            check=True,
        )

        return {CMR_XML: cmr_xml, STAC_JSON: stac_json}


@dataclass(frozen=True, kw_only=True)
class S2CreateManifest(Task):
    """
    Creates the main product manifest.
    Ports: create_manifest
    """

    requires = (CONFIG, OUTPUT_BASE_NAME, CMR_XML)
    provides = (SR_MANIFEST_FILE,)

    def __post_init__(self) -> None:
        validate_command("create_manifest")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        base_name = bundle[OUTPUT_BASE_NAME]

        # Derive Bucket Key (s3://bucket/S30/data/YYYYDOY/base_name)
        parts = base_name.split(".")
        year_doy = parts[3][:7]  # 2020001

        bucket_key = f"s3://{config.output_bucket}/S30/data/{year_doy}/{base_name}"
        manifest_path = config.working_dir / f"{base_name}.json"

        logger.info("Creating Manifest")
        subprocess.run(
            [
                "create_manifest",
                str(config.working_dir),
                str(manifest_path),
                bucket_key,
                "HLSS30",
                base_name,
                config.job_id,
                "false",
            ],
            check=True,
        )

        return {SR_MANIFEST_FILE: manifest_path}


@dataclass(frozen=True, kw_only=True)
class S2ProcessGibs(Task):
    """
    Generates GIBS tiles and manifests.
    Ports: granule_to_gibs, create_manifest
    """

    requires = (CONFIG, OUTPUT_BASE_NAME, SR_MANIFEST_FILE)
    provides = (GIBS_DIR, GIBS_MANIFEST_FILES)

    def __post_init__(self) -> None:
        validate_command("granule_to_gibs")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        base_name = bundle[OUTPUT_BASE_NAME]

        parts = base_name.split(".")
        year_doy = parts[3][:7]

        gibs_dir = config.working_dir / "gibs"
        gibs_dir.mkdir(parents=True, exist_ok=True)
        gibs_bucket_key_root = f"s3://{config.gibs_bucket}/S30/data/{year_doy}"

        logger.info("Generating GIBS tiles")
        subprocess.run(
            ["granule_to_gibs", str(config.working_dir), str(gibs_dir), base_name],
            check=True,
        )

        manifests = []
        for gibs_id_path in gibs_dir.iterdir():
            if gibs_id_path.is_dir():
                gibs_id = gibs_id_path.name
                xmls = list(gibs_id_path.glob("*.xml"))
                if not xmls:
                    continue

                subtile_base = xmls[0].stem
                manifest = gibs_id_path / f"{subtile_base}.json"
                gibs_bucket_key = f"{gibs_bucket_key_root}/{gibs_id}"

                subprocess.run(
                    [
                        "create_manifest",
                        str(gibs_id_path),
                        str(manifest),
                        gibs_bucket_key,
                        "HLSS30",
                        subtile_base,
                        config.job_id,
                        "true",
                    ],
                    check=True,
                )
                manifests.append(manifest)

        return {GIBS_DIR: gibs_dir, GIBS_MANIFEST_FILES: manifests}


@dataclass(frozen=True, kw_only=True)
class S2ProcessVi(Task):
    """
    Generates Vegetation Indices.
    Ports: vi_generate_indices, vi_generate_metadata, vi_generate_stac_items
    """

    requires = (CONFIG, OUTPUT_BASE_NAME, SR_MANIFEST_FILE)
    provides = (VI_DIR, VI_MANIFEST_FILE)

    def __post_init__(self) -> None:
        validate_command("vi_generate_indices")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        base_name = bundle[OUTPUT_BASE_NAME]
        vi_base_name = base_name.replace("HLS.S30", "HLS-VI.S30")

        parts = base_name.split(".")
        year_doy = parts[3][:7]

        vi_dir = config.working_dir / "vi"
        vi_dir.mkdir(parents=True, exist_ok=True)

        vi_bucket_key = (
            f"s3://{config.output_bucket}/S30_VI/data/{year_doy}/{vi_base_name}"
        )

        logger.info("Generating VI files")
        subprocess.run(
            [
                "vi_generate_indices",
                "-i",
                str(config.working_dir),
                "-o",
                str(vi_dir),
                "-s",
                base_name,
            ],
            check=True,
        )

        subprocess.run(
            ["vi_generate_metadata", "-i", str(config.working_dir), "-o", str(vi_dir)],
            check=True,
        )

        subprocess.run(
            [
                "vi_generate_stac_items",
                "--cmr_xml",
                str(vi_dir / f"{vi_base_name}.cmr.xml"),
                "--endpoint",
                "data.lpdaac.earthdatacloud.nasa.gov",
                "--version",
                "020",
                "--out_json",
                str(vi_dir / f"{vi_base_name}_stac.json"),
            ],
            check=True,
        )

        logger.info("Generating VI Manifest")
        manifest = vi_dir / f"{vi_base_name}.json"
        subprocess.run(
            [
                "create_manifest",
                str(vi_dir),
                str(manifest),
                vi_bucket_key,
                "HLSS30_VI",
                vi_base_name,
                config.job_id,
                "false",
            ],
            check=True,
        )

        return {VI_DIR: vi_dir, VI_MANIFEST_FILE: manifest}


@dataclass(frozen=True, kw_only=True)
class UploadAll(Task):
    """
    Handles all S2 uploads (Product, Debug, GIBS, VI).
    Mirrors the upload logic in sentinel.sh.
    """

    requires = (
        CONFIG,
        OUTPUT_BASE_NAME,
        GIBS_DIR,
        GIBS_MANIFEST_FILES,
        VI_DIR,
        VI_MANIFEST_FILE,
        SR_MANIFEST_FILE,
        # Intermediate assets needed for Debug Mode
        RESAMPLED_HDF,
        NBAR_HDF,  # This is the intermediate NBAR before L8Like
    )
    provides = (UPLOAD_COMPLETE,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        base_name = bundle[OUTPUT_BASE_NAME]
        s3 = boto3.client("s3")

        if config.debug_bucket:
            self._upload_debug(s3, bundle, config, base_name)
        else:
            self._upload_production(s3, bundle, config, base_name)

        return {UPLOAD_COMPLETE: True}

    def _upload_production(
        self, s3: S3Client, bundle: AssetBundle, config: EnvConfig, base_name: str
    ) -> None:
        """Handles production upload logic by delegating to specific product handlers."""
        parts = base_name.split(".")
        year_doy = parts[3][:7]

        logger.info("Starting Production Uploads")
        self._upload_main_product(s3, bundle, config, base_name, year_doy)
        self._upload_gibs(s3, bundle, config, year_doy)
        self._upload_vi(s3, bundle, config, base_name, year_doy)

    def _upload_main_product(
        self,
        s3: S3Client,
        bundle: AssetBundle,
        config: EnvConfig,
        base_name: str,
        year_doy: str,
    ) -> None:
        """Uploads standard HLS S30 product files and manifest."""
        logger.info("Uploading Main Product")
        bucket_key_root = f"S30/data/{year_doy}/{base_name}"

        # Include patterns: *.tif, *.xml, *.jpg, *_stac.json
        # Exclude: *fmask.bin.aux.xml
        for f in config.working_dir.iterdir():
            if not f.is_file():
                continue

            # Check Excludes
            if f.name.endswith("fmask.bin.aux.xml"):
                continue

            # Check Includes
            if f.suffix in [".tif", ".xml", ".jpg"] or f.name.endswith("_stac.json"):
                key = f"{bucket_key_root}/{f.name}"
                s3.upload_file(str(f), config.output_bucket, key)

        # Upload Manifest
        manifest = bundle[SR_MANIFEST_FILE]
        if manifest.exists():
            s3.upload_file(
                str(manifest),
                config.output_bucket,
                f"{bucket_key_root}/{manifest.name}",
            )

    def _upload_gibs(
        self, s3: S3Client, bundle: AssetBundle, config: EnvConfig, year_doy: str
    ) -> None:
        """Uploads GIBS tiles and manifests."""
        logger.info("Uploading GIBS")
        gibs_dir = bundle[GIBS_DIR]
        gibs_root_key = f"S30/data/{year_doy}"

        for gibs_id_path in gibs_dir.iterdir():
            if gibs_id_path.is_dir():
                gibs_id = gibs_id_path.name
                target_key = f"{gibs_root_key}/{gibs_id}"

                # Upload content
                for f in gibs_id_path.glob("*"):
                    if f.suffix in [".tif", ".xml"]:
                        s3.upload_file(
                            str(f), config.gibs_bucket, f"{target_key}/{f.name}"
                        )

                # Upload Subtile Manifest
                xmls = list(gibs_id_path.glob("*.xml"))
                if xmls:
                    subtile_base = xmls[0].stem
                    man = gibs_id_path / f"{subtile_base}.json"
                    if man.exists():
                        s3.upload_file(
                            str(man), config.gibs_bucket, f"{target_key}/{man.name}"
                        )

    def _upload_vi(
        self,
        s3: S3Client,
        bundle: AssetBundle,
        config: EnvConfig,
        base_name: str,
        year_doy: str,
    ) -> None:
        """Uploads Vegetation Index files and manifest."""
        logger.info("Uploading VI")
        vi_dir = bundle[VI_DIR]
        vi_base_name = base_name.replace("HLS.S30", "HLS-VI.S30")
        vi_root_key = f"S30_VI/data/{year_doy}/{vi_base_name}"

        for f in vi_dir.iterdir():
            if f.suffix in [".tif", ".xml", ".jpg"] or f.name.endswith("_stac.json"):
                s3.upload_file(str(f), config.output_bucket, f"{vi_root_key}/{f.name}")

        vi_manifest = bundle[VI_MANIFEST_FILE]
        if vi_manifest.exists():
            s3.upload_file(
                str(vi_manifest),
                config.output_bucket,
                f"{vi_root_key}/{vi_manifest.name}",
            )

    def _upload_debug(
        self, s3: S3Client, bundle: AssetBundle, config: EnvConfig, base_name: str
    ) -> None:
        """Handles debug mode uploads."""
        assert config.debug_bucket is not None

        resample_hdf = bundle[RESAMPLED_HDF]
        nbar_intermediate = bundle[NBAR_HDF]

        # Convert intermediate HDFs to COGs for debug using helper
        logger.info("DEBUG: Generating intermediate COGs")
        run_hdf_to_cog(
            input_file=resample_hdf,
            output_dir=config.working_dir,
            product="S30",
            debug_mode=True,
        )
        run_hdf_to_cog(
            input_file=nbar_intermediate,
            output_dir=config.working_dir,
            product="S30",
            debug_mode=True,
        )

        logger.info(f"DEBUG: Uploading all files to {config.debug_bucket}/{base_name}")
        target_root = base_name

        # Recursive copy of working dir
        for f in config.working_dir.rglob("*"):
            if f.is_file():
                rel_path = f.relative_to(config.working_dir)
                key = f"{target_root}/{rel_path}"
                s3.upload_file(str(f), config.debug_bucket, key)
