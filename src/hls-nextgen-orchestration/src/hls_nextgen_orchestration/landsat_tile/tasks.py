from __future__ import annotations

import datetime as dt
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3

from hls_nextgen_orchestration.base import DataSource, Task, TaskFailure
from hls_nextgen_orchestration.utils import validate_command

from .assets import (
    ANGLE_HDF,
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    GIBS_DIR,
    GRIDDED_HDF,
    MANIFEST_FILE,
    NBAR_ANGLE,
    NBAR_INPUT,
    OUTPUT_BASE_NAME,
    OUTPUT_HDF,
    SCENE_TIME,
    STAC_JSON,
    THUMBNAIL_FILE,
    UPLOAD_COMPLETE,
    VI_DIR,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

__all__ = [
    "EnvConfig",
    "EnvSource",
    "ProcessPathRows",
    "RunNbar",
    "ConvertToCogs",
    "CreateThumbnail",
    "CreateMetadata",
    "CreateManifest",
    "ProcessGibs",
    "ProcessVi",
    "UploadAll",
]


# --- Configuration ---


@dataclass(frozen=True)
class EnvConfig:
    """
    Configuration for the environment variables used in the pipeline.
    """

    job_id: str
    pathrow_list: list[str]
    date: str  # YYYY-MM-DD
    mgrs: str
    mgrs_ulx: str
    mgrs_uly: str
    input_bucket: str
    output_bucket: str
    gibs_bucket: str
    debug_bucket: str | None = None

    @property
    def working_dir(self) -> Path:
        return Path(f"/var/scratch/{self.job_id}")

    @property
    def year(self) -> str:
        return self.date[0:4]

    @property
    def day_of_year(self) -> str:
        # Simple DOY calculation from YYYY-MM-DD
        d = dt.datetime.strptime(self.date, "%Y-%m-%d")
        return d.strftime("%j")


# --- Data Source ---


@dataclass(frozen=True)
class EnvSource(DataSource):
    """
    Reads environment variables to configure the processing job.
    """

    def fetch(self) -> dict[Any, Any]:
        pathrows = os.environ.get("PATHROW_LIST", "").split(",")
        config = EnvConfig(
            job_id=os.environ.get("AWS_BATCH_JOB_ID", "local_job"),
            pathrow_list=[p.strip() for p in pathrows if p.strip()],
            date=os.environ.get("DATE", "2020-01-01"),
            mgrs=os.environ.get("MGRS", "T12ABC"),
            mgrs_ulx=os.environ.get("MGRS_ULX", "0"),
            mgrs_uly=os.environ.get("MGRS_ULY", "0"),
            input_bucket=os.environ.get("INPUT_BUCKET", "landsat-pds"),
            output_bucket=os.environ.get("OUTPUT_BUCKET", "processed-data"),
            gibs_bucket=os.environ.get("GIBS_OUTPUT_BUCKET", "gibs-data"),
            debug_bucket=os.environ.get("DEBUG_BUCKET"),
        )

        if not config.working_dir.exists():
            config.working_dir.mkdir(parents=True, exist_ok=True)

        return {CONFIG: config}


# --- Tasks ---


@dataclass(frozen=True)
class ProcessPathRows(Task):
    """
    Downloads input data for each pathrow, extracts scene time,
    and runs landsat-tile / landsat-angle-tile.
    """

    def __post_init__(self) -> None:
        validate_command("extract_landsat_hms.py")
        validate_command("landsat-tile")
        validate_command("landsat-angle-tile")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        os.chdir(config.working_dir)

        s3: S3Client = boto3.client("s3")
        year, month, day = config.date.split("-")
        scene_time = ""

        # Download and Process Loop
        for idx, pathrow in enumerate(config.pathrow_list):
            basename = f"{config.date}_{pathrow}"
            landsat_ac = f"{basename}.hdf"
            landsat_sz_angle = f"{basename}_SZA.img"
            input_key = f"{year}-{month}-{day}/{pathrow}"

            logging.info(
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
                    # Skip directory placeholders
                    if key.endswith("/"):
                        continue

                    # We flatten the structure to match 'aws s3 cp ... .' behavior
                    # expecting inputs to be in working_dir for the tools below
                    filename = Path(key).name
                    dest_path = config.working_dir / filename

                    # Only download if not already present (optional optimization)
                    if not dest_path.exists():
                        s3.download_file(config.input_bucket, key, str(dest_path))

            if idx == 0:
                # Extract scene time from the first image
                res = subprocess.run(
                    ["extract_landsat_hms.py", landsat_ac],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                scene_time = res.stdout.strip()

            # Construct Output Names (needed for tile commands)
            hls_version = "v2.0"
            nbar_basename = f"{config.mgrs}.{config.year}{config.day_of_year}.{scene_time}.{hls_version}"
            nbar_name = f"HLS.L30.{nbar_basename}"
            nbar_input = config.working_dir / f"{nbar_name}.hdf"
            nbar_angle = config.working_dir / f"L8ANGLE.{nbar_basename}.hdf"

            logging.info(f"Running landsat-tile for {pathrow}")
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

            logging.info(f"Running landsat-angle-tile for {pathrow}")
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

        # Final check
        hls_version = "v2.0"
        nbar_basename = f"{config.mgrs}.{config.year}{config.day_of_year}.{scene_time}.{hls_version}"
        nbar_name = f"HLS.L30.{nbar_basename}"
        nbar_input = config.working_dir / f"{nbar_name}.hdf"
        nbar_angle = config.working_dir / f"L8ANGLE.{nbar_basename}.hdf"
        output_basename = f"T{config.mgrs}.{config.year}{config.day_of_year}T{scene_time}.{hls_version}"

        if not nbar_input.exists() or not nbar_angle.exists():
            raise TaskFailure("No output tile produced", exit_code=5)

        return {
            NBAR_INPUT: nbar_input,
            NBAR_ANGLE: nbar_angle,
            SCENE_TIME: scene_time,
            OUTPUT_BASE_NAME: output_basename,
        }


@dataclass(frozen=True)
class RunNbar(Task):
    """
    Runs NBAR correction and renames output files.
    """

    def __post_init__(self) -> None:
        validate_command("landsat-nbar")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        nbar_input: Path = inputs[NBAR_INPUT]
        nbar_angle: Path = inputs[NBAR_ANGLE]
        output_basename: str = inputs[OUTPUT_BASE_NAME]
        scene_time: str = inputs[SCENE_TIME]

        # Reconstruct implicit nbar_cfactor name required by C code logic
        hls_version = "v2.0"
        nbar_basename = f"{config.mgrs}.{config.year}{config.day_of_year}.{scene_time}.{hls_version}"
        nbar_cfactor = config.working_dir / f"CFACTOR.{nbar_basename}.hdf"
        gridded_output = config.working_dir / "gridded.hdf"

        logging.info("Running NBAR")
        # Copy intermediate gridded output for debugging
        if nbar_input.exists():
            shutil.copy(nbar_input, gridded_output)

        subprocess.run(
            ["landsat-nbar", str(nbar_input), str(nbar_angle), str(nbar_cfactor)],
            check=True,
        )

        # Rename outputs
        output_name = f"HLS.L30.{output_basename}"
        output_hdf = config.working_dir / f"{output_name}.hdf"
        angle_output_final = config.working_dir / f"{output_name}.ANGLE.hdf"

        logging.info("Renaming NBAR outputs")
        nbar_input.rename(output_hdf)
        if nbar_input.with_suffix(".hdf.hdr").exists():
            nbar_input.with_suffix(".hdf.hdr").rename(
                output_hdf.with_suffix(".hdf.hdr")
            )
        elif Path(f"{nbar_input}.hdr").exists():
            Path(f"{nbar_input}.hdr").rename(f"{output_hdf}.hdr")

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

    def __post_init__(self) -> None:
        validate_command("hdf_to_cog")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        output_hdf: Path = inputs[OUTPUT_HDF]
        angle_hdf: Path = inputs[ANGLE_HDF]

        os.chdir(config.working_dir)

        logging.info("Converting to COGs")
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

    def __post_init__(self) -> None:
        validate_command("create_thumbnail")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        output_basename: str = inputs[OUTPUT_BASE_NAME]
        output_name = f"HLS.L30.{output_basename}"
        thumb_name = f"{output_name}.jpg"

        os.chdir(config.working_dir)

        logging.info("Creating thumbnail")
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

    def __post_init__(self) -> None:
        validate_command("create_metadata")
        validate_command("cmr_to_stac_item")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        output_hdf: Path = inputs[OUTPUT_HDF]
        output_basename: str = inputs[OUTPUT_BASE_NAME]
        output_name = f"HLS.L30.{output_basename}"

        os.chdir(config.working_dir)

        # Metadata
        logging.info("Creating metadata")
        meta_xml = f"{output_name}.cmr.xml"
        subprocess.run(
            ["create_metadata", str(output_hdf), "--save", meta_xml],
            check=True,
        )

        # STAC
        logging.info("Creating STAC metadata")
        stac_json = f"{output_name}_stac.json"
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
class CreateManifest(Task):
    """
    Generates the product manifest file.
    """

    def __post_init__(self) -> None:
        validate_command("create_manifest")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        # Dependencies to ensure previous steps are done
        _ = inputs[COGS_CREATED]
        _ = inputs[THUMBNAIL_FILE]
        _ = inputs[CMR_XML]

        config: EnvConfig = inputs[CONFIG]
        output_basename: str = inputs[OUTPUT_BASE_NAME]
        output_name = f"HLS.L30.{output_basename}"
        bucket_key = f"s3://{config.output_bucket}/L30/data/{config.year}{config.day_of_year}/{output_name}"

        os.chdir(config.working_dir)

        logging.info("Generating manifest")
        manifest_name = f"{output_name}.json"
        subprocess.run(
            [
                "create_manifest",
                str(config.working_dir),
                manifest_name,
                bucket_key,
                "HLSL30",
                output_name,
                config.job_id,
                "false",
            ],
            check=True,
        )
        return {MANIFEST_FILE: config.working_dir / manifest_name}


@dataclass(frozen=True)
class ProcessGibs(Task):
    """
    Generates GIBS browse subtiles and manifests.
    """

    def __post_init__(self) -> None:
        validate_command("granule_to_gibs")
        validate_command("create_manifest")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        output_basename: str = inputs[OUTPUT_BASE_NAME]
        output_name = f"HLS.L30.{output_basename}"

        gibs_dir = config.working_dir / "gibs"
        gibs_dir.mkdir(parents=True, exist_ok=True)

        gibs_bucket_key = (
            f"s3://{config.gibs_bucket}/L30/data/{config.year}{config.day_of_year}"
        )

        logging.info("Generating GIBS browse subtiles")
        subprocess.run(
            ["granule_to_gibs", str(config.working_dir), str(gibs_dir), output_name],
            check=True,
        )

        # Iterate through generated dirs
        for gibs_id_dir in gibs_dir.iterdir():
            if gibs_id_dir.is_dir():
                gibs_id = gibs_id_dir.name
                logging.info(f"Processing gibs id {gibs_id}")

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

        return {GIBS_DIR: gibs_dir}


@dataclass(frozen=True)
class ProcessVi(Task):
    """
    Generates Vegetation Index (VI) files and metadata.
    """

    def __post_init__(self) -> None:
        validate_command("vi_generate_indices")
        validate_command("vi_generate_metadata")
        validate_command("vi_generate_stac_items")
        validate_command("create_manifest")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        output_basename: str = inputs[OUTPUT_BASE_NAME]

        output_name = f"HLS.L30.{output_basename}"
        vi_output_name = f"HLS-VI.L30.{output_basename}"

        vi_dir = config.working_dir / "vi"
        vi_dir.mkdir(parents=True, exist_ok=True)

        vi_bucket_key = f"s3://{config.output_bucket}/L30_VI/data/{config.year}{config.day_of_year}/{vi_output_name}"

        logging.info("Generating VI files")
        subprocess.run(
            [
                "vi_generate_indices",
                "-i",
                str(config.working_dir),
                "-o",
                str(vi_dir),
                "-s",
                output_name,
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

        logging.info("Generating VI manifest")
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

        return {VI_DIR: vi_dir}


@dataclass(frozen=True)
class UploadAll(Task):
    """
    Handles all uploads (Product, Debug, GIBS, VI) based on configuration.
    """

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        output_basename: str = inputs[OUTPUT_BASE_NAME]
        gibs_dir: Path = inputs[GIBS_DIR]
        vi_dir: Path = inputs[VI_DIR]
        gridded_hdf: Path = inputs[GRIDDED_HDF]
        manifest_file: Path = inputs[MANIFEST_FILE]

        s3: S3Client = boto3.client("s3")

        if not config.debug_bucket:
            self._upload_production(
                s3, config, output_basename, gibs_dir, vi_dir, manifest_file
            )
        else:
            # Check debug dependency here since we only use it in this branch
            validate_command("hdf_to_cog")
            self._upload_debug(s3, config, output_basename, gridded_hdf)

        return {UPLOAD_COMPLETE: True}

    def _upload_production(
        self,
        s3: S3Client,
        config: EnvConfig,
        output_basename: str,
        gibs_dir: Path,
        vi_dir: Path,
        manifest_file: Path,
    ) -> None:
        """Helper to handle all production upload logic."""
        logging.info("Uploading Main Product to S3")
        self._upload_main_product(s3, config, output_basename, manifest_file)

        logging.info("Uploading GIBS tiles")
        self._upload_gibs(s3, config, gibs_dir)

        logging.info("Uploading VI files")
        self._upload_vi(s3, config, output_basename, vi_dir)

    def _upload_main_product(
        self,
        s3: S3Client,
        config: EnvConfig,
        output_basename: str,
        manifest_file: Path,
    ) -> None:
        """Uploads main product files and manifest."""
        output_name = f"HLS.L30.{output_basename}"
        bucket_key_path = f"L30/data/{config.year}{config.day_of_year}/{output_name}"
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
        self, s3: S3Client, config: EnvConfig, output_basename: str, vi_dir: Path
    ) -> None:
        """Uploads Vegetation Index files."""
        vi_output_name = f"HLS-VI.L30.{output_basename}"
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
        self, s3: S3Client, config: EnvConfig, output_basename: str, gridded_hdf: Path
    ) -> None:
        """Handles debug mode uploads."""
        logging.info("DEBUG MODE: Creating gridded debug COG")
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

        output_name = f"HLS.L30.{output_basename}"
        logging.info("Copying all files to debug bucket")
        target_key = f"{output_name}"

        # Recursive upload of working_dir
        for f in config.working_dir.rglob("*"):
            if f.is_file():
                rel_path = f.relative_to(config.working_dir)
                key = f"{target_key}/{rel_path}"
                s3.upload_file(str(f), config.debug_bucket, key)
