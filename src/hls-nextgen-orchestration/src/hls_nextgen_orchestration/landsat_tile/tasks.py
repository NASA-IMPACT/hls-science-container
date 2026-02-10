from __future__ import annotations

import datetime as dt
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3

from hls_nextgen_orchestration.base import (
    Asset,
    AssetBundle,
    DataSource,
    Task,
    TaskFailure,
)
from hls_nextgen_orchestration.granules import HlsGranule
from hls_nextgen_orchestration.utils import validate_command

from .assets import (
    ANGLE_HDF,
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    GIBS_DIR,
    GIBS_MANIFEST_FILES,
    GRIDDED_HDF,
    NBAR_ANGLE,
    NBAR_INPUT,
    OUTPUT_BASE_NAME,
    OUTPUT_HDF,
    PATHROW_IMAGES,
    SCENE_TIME,
    SR_MANIFEST_FILE,
    STAC_JSON,
    THUMBNAIL_FILE,
    UPLOAD_COMPLETE,
    VI_DIR,
    VI_MANIFEST_FILE,
    EnvConfig,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


def get_nbar_names(granule: HlsGranule) -> dict[str, str]:
    """
    Generate strict NBAR-compatible filenames from a granule.

    The C-based NBAR code requires specific filename formats that differ
    slightly from the standard HLS granule ID (specifically using dots
    instead of 'T' separators for time components).

    Parameters
    ----------
    granule : HlsGranule
        The HLS granule object.

    Returns
    -------
    dict[str, str]
        Dictionary containing filenames for 'product', 'angle', and 'cfactor'.
    """
    time_str = granule.acquisition_time.strftime("%H%M%S")
    year_doy = granule.acquisition_time.strftime("%Y%j")

    # Format: T12ABC.2022001.123456.v2.0
    basename = (
        f"{granule.tile_id}.{year_doy}.{time_str}."
        f"{granule.version_major}.{granule.version_minor}"
    )

    return {
        "product": f"HLS.L30.{basename}.hdf",
        "angle": f"L8ANGLE.{basename}.hdf",
        "cfactor": f"CFACTOR.{basename}.hdf",
    }


# --- Data Source ---


@dataclass(frozen=True)
class EnvSource(DataSource):
    """Reads environment variables to configure the processing job."""

    provides = (CONFIG,)

    scratch_dir: Path = field(
        default_factory=lambda: Path(os.getenv("SCRATCH_DIR", "/var/scratch"))
    )
    working_dir: Path | None = field(
        default_factory=lambda: Path(d) if (d := os.getenv("WORKING_DIR")) else None
    )
    purge_working_dir: bool = True

    def fetch(self) -> AssetBundle:
        """
        Fetch configuration from environment variables.

        Returns
        -------
        dict
            Dictionary containing the EnvConfig object.
        """
        job_id = os.environ.get("AWS_BATCH_JOB_ID", "local_job")
        pathrows = os.environ["PATHROW_LIST"].split(",")

        working_dir = (
            self.working_dir if self.working_dir else self.scratch_dir / job_id
        )

        config = EnvConfig(
            job_id=job_id,
            pathrow_list=[p.strip() for p in pathrows if p.strip()],
            date=dt.datetime.strptime(os.environ["DATE"], "%Y-%m-%d").date(),
            mgrs=os.environ["MGRS"],
            mgrs_ulx=os.environ["MGRS_ULX"],
            mgrs_uly=os.environ["MGRS_ULY"],
            input_bucket=os.environ["INPUT_BUCKET"],
            output_bucket=os.environ["OUTPUT_BUCKET"],
            gibs_bucket=os.environ["GIBS_OUTPUT_BUCKET"],
            working_dir=working_dir,
            debug_bucket=os.environ.get("DEBUG_BUCKET"),
        )

        if config.working_dir.exists() and self.purge_working_dir:
            logger.info(f"Deleting pre-existing {config.working_dir=}")
            shutil.rmtree(config.working_dir)

        if not config.working_dir.exists():
            config.working_dir.mkdir(parents=True, exist_ok=True)

        return {CONFIG: config}


# --- Tasks ---
@dataclass(frozen=True)
class DownloadPathRows(Task):
    """
    Downloads all tile inputs for path/rows
    """

    requires = (CONFIG,)
    provides = (PATHROW_IMAGES,)

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        os.chdir(config.working_dir)

        s3: S3Client = boto3.client("s3")

        # Format date for S3 keys and filenames
        date_str = config.date.strftime("%Y-%m-%d")
        year, month, day = date_str.split("-")

        # Download loop
        pathrows_to_images: dict[str, list[Path]] = {
            pr: [] for pr in config.pathrow_list
        }
        for pathrow in pathrows_to_images:
            input_key = f"{year}-{month}-{day}/{pathrow}"

            logger.info(
                f"Downloading files from s3://{config.input_bucket}/{input_key}..."
            )

            # Recursive download using boto3
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=config.input_bucket, Prefix=input_key
            ):
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if key.endswith("/"):
                        continue

                    filename = Path(key).name
                    dest_path = config.working_dir / filename

                    s3.download_file(config.input_bucket, key, str(dest_path))
                    pathrows_to_images[pathrow].append(dest_path)

        return {PATHROW_IMAGES: pathrows_to_images}


@dataclass(frozen=True)
class LocalPathRows(Task):
    """
    Locate pre-downloaded Landsat path/row granules
    """

    requires = (CONFIG,)
    provides = (PATHROW_IMAGES,)

    local_pathrows_dir: Path

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]

        date_str = config.date.strftime("%Y-%m-%d")
        year, month, day = date_str.split("-")

        # Find data
        pathrows_to_images: dict[str, list[Path]] = {
            pr: [] for pr in config.pathrow_list
        }
        for pathrow in pathrows_to_images:
            images = list(self.local_pathrows_dir.glob(f"{date_str}_{pathrow}*"))
            logger.info(f"Copying {len(images)} for {pathrow=} to {config.working_dir}")
            for image in images:
                dest = config.working_dir / image.name
                shutil.copy(image, dest)
                pathrows_to_images[pathrow].append(dest)

        return {PATHROW_IMAGES: pathrows_to_images}


@dataclass(frozen=True)
class ProcessPathRows(Task):
    """
    Extracts scene time and runs landsat-tile / landsat-angle-tile.
    """

    requires = (CONFIG, PATHROW_IMAGES)
    provides = (NBAR_INPUT, NBAR_ANGLE, SCENE_TIME, OUTPUT_BASE_NAME)

    def __post_init__(self) -> None:
        # FIXME: import "extract_landsat_hms" as Python function to run
        validate_command("extract_landsat_hms.py")
        validate_command("landsat-tile")
        validate_command("landsat-angle-tile")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]

        os.chdir(config.working_dir)

        # Format date for S3 keys and filenames
        date_str = config.date.strftime("%Y-%m-%d")
        year, month, day = date_str.split("-")
        scene_time_str = ""

        # Download and Process Loop
        for idx, pathrow in enumerate(config.pathrow_list):
            basename = f"{date_str}_{pathrow}"
            landsat_ac = f"{basename}.hdf"
            landsat_sz_angle = f"{basename}_SZA.img"

            if idx == 0:
                # Extract scene time from the first image
                res = subprocess.run(
                    ["extract_landsat_hms.py", landsat_ac],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                scene_time_str = res.stdout.strip()

            # Parse datetime for HlsGranule
            acq_time = dt.datetime.strptime(
                f"{date_str} {scene_time_str}", "%Y-%m-%d %H%M%S"
            )

            granule = HlsGranule(
                product="HLS",
                sensor="L30",
                tile_id=config.mgrs,
                acquisition_time=acq_time,
            )
            full_granule_id = granule.to_str()

            # Use helper for strict NBAR naming (legacy format for C binaries)
            nbar_names = get_nbar_names(granule)
            nbar_input = config.working_dir / nbar_names["product"]
            nbar_angle = config.working_dir / nbar_names["angle"]

            logger.info(f"Running landsat-tile for {pathrow}")
            subprocess.run(
                [
                    "landsat-tile",
                    config.mgrs,
                    config.mgrs_ulx,
                    config.mgrs_uly,
                    "NONE",
                    "NONE",
                    landsat_ac,
                    str(nbar_input),
                ],
                check=True,
            )

            logger.info(f"Running landsat-angle-tile for {pathrow}")
            subprocess.run(
                [
                    "landsat-angle-tile",
                    config.mgrs,
                    config.mgrs_ulx,
                    config.mgrs_uly,
                    landsat_sz_angle,
                    str(nbar_angle),
                ],
                check=True,
            )

        # Re-construct granule logic to ensure availability of variables outside loop
        acq_time = dt.datetime.strptime(
            f"{date_str} {scene_time_str}", "%Y-%m-%d %H%M%S"
        )
        granule = HlsGranule(
            product="HLS",
            sensor="L30",
            tile_id=config.mgrs,
            acquisition_time=acq_time,
        )
        full_granule_id = granule.to_str()
        nbar_names = get_nbar_names(granule)

        nbar_input = config.working_dir / nbar_names["product"]
        nbar_angle = config.working_dir / nbar_names["angle"]

        if not nbar_input.exists() or not nbar_angle.exists():
            raise TaskFailure("No output tile produced", exit_code=5)

        return {
            NBAR_INPUT: nbar_input,
            NBAR_ANGLE: nbar_angle,
            SCENE_TIME: scene_time_str,
            OUTPUT_BASE_NAME: full_granule_id,
        }


@dataclass(frozen=True)
class RunNbar(Task):
    """
    Runs NBAR correction and renames output files.
    """

    requires = (
        CONFIG,
        NBAR_INPUT,
        NBAR_ANGLE,
        SCENE_TIME,
        OUTPUT_BASE_NAME,
    )
    provides = (OUTPUT_HDF, ANGLE_HDF, GRIDDED_HDF)

    def __post_init__(self) -> None:
        validate_command("landsat-nbar")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        nbar_input: Path = inputs[NBAR_INPUT]
        nbar_angle: Path = inputs[NBAR_ANGLE]
        granule_id: str = inputs[OUTPUT_BASE_NAME]  # This is the full HLS ID

        # Reconstruct granule and get legacy NBAR names
        granule = HlsGranule.from_str(granule_id)
        nbar_names = get_nbar_names(granule)

        nbar_cfactor = config.working_dir / nbar_names["cfactor"]
        gridded_output = config.working_dir / "gridded.hdf"

        logger.info("Running NBAR")
        # Copy intermediate gridded output for debugging
        if nbar_input.exists():
            shutil.copy(nbar_input, gridded_output)

        subprocess.run(
            ["landsat-nbar", str(nbar_input), str(nbar_angle), str(nbar_cfactor)],
            check=True,
        )

        # Rename outputs to the final Standard HLS ID
        output_hdf = config.working_dir / f"{granule_id}.hdf"
        angle_output_final = config.working_dir / f"{granule_id}.ANGLE.hdf"

        logger.info("Renaming NBAR outputs")
        if nbar_input != output_hdf:
            nbar_input.rename(output_hdf)

        # Handle HDR files
        if nbar_input.with_suffix(".hdf.hdr").exists():
            nbar_input.with_suffix(".hdf.hdr").rename(
                output_hdf.with_suffix(".hdf.hdr")
            )
        elif Path(f"{nbar_input}.hdr").exists():
            Path(f"{nbar_input}.hdr").rename(f"{output_hdf}.hdr")

        if nbar_angle != angle_output_final:
            nbar_angle.rename(angle_output_final)

        return {
            OUTPUT_HDF: output_hdf,
            ANGLE_HDF: angle_output_final,
            GRIDDED_HDF: gridded_output,
        }


@dataclass(frozen=True)
class ConvertToCogs(Task):
    """
    Converts HDF outputs to COG format.
    """

    requires = (CONFIG, OUTPUT_HDF, ANGLE_HDF)
    provides = (COGS_CREATED,)

    def __post_init__(self) -> None:
        validate_command("hdf_to_cog")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        output_hdf: Path = inputs[OUTPUT_HDF]
        angle_hdf: Path = inputs[ANGLE_HDF]

        os.chdir(config.working_dir)

        logger.info("Converting to COGs")
        subprocess.run(
            [
                "hdf_to_cog",
                str(output_hdf),
                "--output-dir",
                str(config.working_dir),
                "--product",
                "L30",
            ],
            check=True,
        )
        subprocess.run(
            [
                "hdf_to_cog",
                str(angle_hdf),
                "--output-dir",
                str(config.working_dir),
                "--product",
                "L30_ANGLES",
            ],
            check=True,
        )
        return {COGS_CREATED: True}


@dataclass(frozen=True)
class CreateThumbnail(Task):
    """
    Creates a thumbnail image for the product.
    """

    requires = (CONFIG, COGS_CREATED, OUTPUT_BASE_NAME)
    provides = (THUMBNAIL_FILE,)

    def __post_init__(self) -> None:
        validate_command("create_thumbnail")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        granule_id: str = inputs[OUTPUT_BASE_NAME]
        thumb_name = f"{granule_id}.jpg"

        os.chdir(config.working_dir)

        logger.info("Creating thumbnail")
        subprocess.run(
            [
                "create_thumbnail",
                "-i",
                str(config.working_dir),
                "-o",
                thumb_name,
                "-s",
                "L30",
            ],
            check=True,
        )
        return {THUMBNAIL_FILE: config.working_dir / thumb_name}


@dataclass(frozen=True)
class CreateMetadata(Task):
    """
    Generates CMR XML and STAC JSON metadata.
    """

    requires = (CONFIG, COGS_CREATED, OUTPUT_HDF, OUTPUT_BASE_NAME)
    provides = (CMR_XML, STAC_JSON)

    def __post_init__(self) -> None:
        validate_command("create_metadata")
        validate_command("cmr_to_stac_item")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        output_hdf: Path = inputs[OUTPUT_HDF]
        granule_id: str = inputs[OUTPUT_BASE_NAME]

        os.chdir(config.working_dir)

        # Metadata
        logger.info("Creating metadata")
        meta_xml = f"{granule_id}.cmr.xml"
        subprocess.run(
            ["create_metadata", str(output_hdf), "--save", meta_xml],
            check=True,
        )

        # STAC
        logger.info("Creating STAC metadata")
        stac_json = f"{granule_id}_stac.json"
        subprocess.run(
            [
                "cmr_to_stac_item",
                meta_xml,
                stac_json,
                "data.lpdaac.earthdatacloud.nasa.gov",
                "020",
            ],
            check=True,
        )
        return {
            CMR_XML: config.working_dir / meta_xml,
            STAC_JSON: config.working_dir / stac_json,
        }


@dataclass(frozen=True)
class CreateSRManifest(Task):
    """
    Generates the HLS surface reflectance product manifest file.
    """

    requires = (
        CONFIG,
        OUTPUT_BASE_NAME,
        COGS_CREATED,
        THUMBNAIL_FILE,
        CMR_XML,
    )
    provides = (SR_MANIFEST_FILE,)

    def __post_init__(self) -> None:
        validate_command("create_manifest")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        granule_id: str = inputs[OUTPUT_BASE_NAME]
        bucket_key = f"s3://{config.output_bucket}/L30/data/{config.year}{config.day_of_year}/{granule_id}"

        os.chdir(config.working_dir)

        logger.info("Generating manifest")
        manifest_name = f"{granule_id}.json"
        subprocess.run(
            [
                "create_manifest",
                str(config.working_dir),
                manifest_name,
                bucket_key,
                "HLSL30",
                granule_id,
                config.job_id,
                "false",
            ],
            check=True,
        )
        return {SR_MANIFEST_FILE: config.working_dir / manifest_name}


@dataclass(frozen=True)
class ProcessGibs(Task):
    """
    Generates GIBS browse subtiles and manifests.
    """

    requires = (CONFIG, COGS_CREATED, SR_MANIFEST_FILE, OUTPUT_BASE_NAME)
    provides = (GIBS_DIR, GIBS_MANIFEST_FILES)

    def __post_init__(self) -> None:
        validate_command("granule_to_gibs")
        validate_command("create_manifest")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        granule_id: str = inputs[OUTPUT_BASE_NAME]

        gibs_dir = config.working_dir / "gibs"
        gibs_dir.mkdir(parents=True, exist_ok=True)

        gibs_bucket_key = (
            f"s3://{config.gibs_bucket}/L30/data/{config.year}{config.day_of_year}"
        )

        logger.info("Generating GIBS browse subtiles")
        subprocess.run(
            ["granule_to_gibs", str(config.working_dir), str(gibs_dir), granule_id],
            check=True,
        )

        # Iterate through generated dirs
        gibs_manifest_files = []
        for gibs_id_dir in gibs_dir.iterdir():
            if gibs_id_dir.is_dir():
                gibs_id = gibs_id_dir.name
                logger.info(f"Processing gibs id {gibs_id}")

                xml_files = list(gibs_id_dir.glob("*.xml"))
                if not xml_files:
                    continue

                xml = xml_files[0]
                subtile_basename = xml.stem
                subtile_manifest = gibs_id_dir / f"{subtile_basename}.json"
                gibs_id_bucket_key = f"{gibs_bucket_key}/{gibs_id}"

                subprocess.run(
                    [
                        "create_manifest",
                        str(gibs_id_dir),
                        str(subtile_manifest),
                        gibs_id_bucket_key,
                        "HLSL30",
                        subtile_basename,
                        config.job_id,
                        "true",
                    ],
                    check=True,
                )
                gibs_manifest_files.append(subtile_manifest)

        return {GIBS_DIR: gibs_dir, GIBS_MANIFEST_FILES: gibs_manifest_files}


@dataclass(frozen=True)
class ProcessVi(Task):
    """
    Generates Vegetation Index (VI) files and metadata.
    """

    requires = (CONFIG, COGS_CREATED, THUMBNAIL_FILE, OUTPUT_BASE_NAME)
    provides = (VI_DIR, VI_MANIFEST_FILE)

    def __post_init__(self) -> None:
        validate_command("vi_generate_indices")
        validate_command("vi_generate_metadata")
        validate_command("vi_generate_stac_items")
        validate_command("create_manifest")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        granule_id: str = inputs[OUTPUT_BASE_NAME]

        # NOTE: VI ID convention is usually HLS-VI.L30...
        # We can construct it from the base ID
        # HLS.L30.T.... -> HLS-VI.L30.T....
        vi_output_name = granule_id.replace("HLS.L30", "HLS-VI.L30")

        vi_dir = config.working_dir / "vi"
        vi_dir.mkdir(parents=True, exist_ok=True)

        vi_bucket_key = f"s3://{config.output_bucket}/L30_VI/data/{config.year}{config.day_of_year}/{vi_output_name}"

        logger.info("Generating VI files")
        subprocess.run(
            [
                "vi_generate_indices",
                "-i",
                str(config.working_dir),
                "-o",
                str(vi_dir),
                "-s",
                granule_id,
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
                str(vi_dir / f"{vi_output_name}.cmr.xml"),
                "--endpoint",
                "data.lpdaac.earthdatacloud.nasa.gov",
                "--version",
                "020",
                "--out_json",
                str(vi_dir / f"{vi_output_name}_stac.json"),
            ],
            check=True,
        )

        logger.info("Generating VI manifest")
        vi_manifest = vi_dir / f"{vi_output_name}.json"
        subprocess.run(
            [
                "create_manifest",
                str(vi_dir),
                str(vi_manifest),
                vi_bucket_key,
                "HLSL30_VI",
                vi_output_name,
                config.job_id,
                "false",
            ],
            check=True,
        )

        return {VI_DIR: vi_dir, VI_MANIFEST_FILE: vi_manifest}


@dataclass(frozen=True)
class UploadAll(Task):
    """
    Handles all uploads (Product, Debug, GIBS, VI) based on configuration.
    """

    requires = (
        CONFIG,
        OUTPUT_BASE_NAME,
        GIBS_DIR,
        GIBS_MANIFEST_FILES,
        GRIDDED_HDF,
        SR_MANIFEST_FILE,
        VI_DIR,
        VI_MANIFEST_FILE,
    )
    provides = (UPLOAD_COMPLETE,)

    def __post_init__(self) -> None:
        # Needed for debug mode
        validate_command("hdf_to_cog")

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]
        granule_id: str = inputs[OUTPUT_BASE_NAME]
        gibs_dir: Path = inputs[GIBS_DIR]
        vi_dir: Path = inputs[VI_DIR]
        gridded_hdf: Path = inputs[GRIDDED_HDF]
        manifest_file: Path = inputs[SR_MANIFEST_FILE]

        s3: S3Client = boto3.client("s3")

        if not config.debug_bucket:
            self._upload_production(
                s3, config, granule_id, gibs_dir, vi_dir, manifest_file
            )
        else:
            self._upload_debug(s3, config, granule_id, gridded_hdf)

        return {UPLOAD_COMPLETE: True}

    def _upload_production(
        self,
        s3: S3Client,
        config: EnvConfig,
        granule_id: str,
        gibs_dir: Path,
        vi_dir: Path,
        manifest_file: Path,
    ) -> None:
        """Helper to handle all production upload logic."""
        logger.info("Uploading Main Product to S3")
        self._upload_main_product(s3, config, granule_id, manifest_file)

        logger.info("Uploading GIBS tiles")
        self._upload_gibs(s3, config, gibs_dir)

        logger.info("Uploading VI files")
        self._upload_vi(s3, config, granule_id, vi_dir)

    def _upload_main_product(
        self,
        s3: S3Client,
        config: EnvConfig,
        granule_id: str,
        manifest_file: Path,
    ) -> None:
        """Uploads main product files and manifest."""
        bucket_key_path = f"L30/data/{config.year}{config.day_of_year}/{granule_id}"
        include_patterns = ["*.tif", "*.xml", "*.jpg", "*_stac.json"]

        for pattern in include_patterns:
            for f in config.working_dir.glob(pattern):
                key = f"{bucket_key_path}/{f.name}"
                s3.upload_file(str(f), config.output_bucket, key)

        if manifest_file.exists():
            s3.upload_file(
                str(manifest_file),
                config.output_bucket,
                f"{bucket_key_path}/{manifest_file.name}",
            )

    def _upload_gibs(self, s3: S3Client, config: EnvConfig, gibs_dir: Path) -> None:
        """Uploads GIBS tiles and manifests."""
        gibs_bucket_key_base = f"L30/data/{config.year}{config.day_of_year}"

        for gibs_id_dir in gibs_dir.iterdir():
            if gibs_id_dir.is_dir():
                gibs_id = gibs_id_dir.name
                target_key = f"{gibs_bucket_key_base}/{gibs_id}"

                # Upload TIF/XML
                for ext in ["*.tif", "*.xml"]:
                    for f in gibs_id_dir.glob(ext):
                        s3.upload_file(
                            str(f), config.gibs_bucket, f"{target_key}/{f.name}"
                        )

                # Upload Manifest
                # Assumes there is at least one XML file to derive stem from
                xml_files = list(gibs_id_dir.glob("*.xml"))
                if xml_files:
                    manifest_name = f"{xml_files[0].stem}.json"
                    manifest_path = gibs_id_dir / manifest_name
                    if manifest_path.exists():
                        s3.upload_file(
                            str(manifest_path),
                            config.gibs_bucket,
                            f"{target_key}/{manifest_name}",
                        )

    def _upload_vi(
        self, s3: S3Client, config: EnvConfig, granule_id: str, vi_dir: Path
    ) -> None:
        """Uploads Vegetation Index files."""
        vi_output_name = granule_id.replace("HLS.L30", "HLS-VI.L30")
        vi_bucket_key_path = (
            f"L30_VI/data/{config.year}{config.day_of_year}/{vi_output_name}"
        )
        include_patterns = ["*.tif", "*.xml", "*.jpg", "*_stac.json"]

        for pattern in include_patterns:
            for f in vi_dir.glob(pattern):
                key = f"{vi_bucket_key_path}/{f.name}"
                s3.upload_file(str(f), config.output_bucket, key)

        vi_manifest_name = f"{vi_output_name}.json"
        vi_manifest = vi_dir / vi_manifest_name
        if vi_manifest.exists():
            s3.upload_file(
                str(vi_manifest),
                config.output_bucket,
                f"{vi_bucket_key_path}/{vi_manifest_name}",
            )

    def _upload_debug(
        self, s3: S3Client, config: EnvConfig, granule_id: str, gridded_hdf: Path
    ) -> None:
        """Handles debug mode uploads."""
        logger.info("DEBUG MODE: Creating gridded debug COG")
        assert config.debug_bucket is not None
        subprocess.run(
            [
                "hdf_to_cog",
                str(gridded_hdf),
                "--output-dir",
                str(config.working_dir),
                "--product",
                "L30",
                "--debug-mode",
            ],
            check=False,
        )

        logger.info("Copying all files to debug bucket")
        target_key = f"{granule_id}"

        # Recursive upload of working_dir
        for f in config.working_dir.rglob("*"):
            if f.is_file():
                rel_path = f.relative_to(config.working_dir)
                key = f"{target_key}/{rel_path}"
                s3.upload_file(str(f), config.debug_bucket, key)
