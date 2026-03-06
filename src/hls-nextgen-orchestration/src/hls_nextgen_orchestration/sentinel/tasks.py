"""Sentinel-2 processing tasks

These mirror the "sentinel.sh" script
"""

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
    MergeTask,
    Task,
)
from hls_nextgen_orchestration.common import Paths
from hls_nextgen_orchestration.common.commands import run_hdf_to_cog
from hls_nextgen_orchestration.granules import HlsGranule, Sentinel2Granule
from hls_nextgen_orchestration.utils import validate_command
from hls_nextgen_orchestration.version import HLS_VERSION, HlsVersion

from .assets import (
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    CONSOLIDATED_ANGLE_HDF,
    CONSOLIDATED_SR_HDF,
    FINAL_OUTPUT_HDF,
    GIBS_DIR,
    GIBS_MANIFEST_FILES,
    NBAR_HDF,
    NBAR_INPUT_HDF,
    OUTPUT_BASE_NAME,
    RENAMED_ANGLE_HDF,
    RENAMED_HDF,
    RESAMPLED_HDF,
    SR_MANIFEST_FILE,
    STAC_JSON,
    THUMBNAIL_FILE,
    UPLOAD_COMPLETE,
    VI_DIR,
    VI_MANIFEST_FILE,
    EnvConfig,
    angle_hdf_asset,
    trimmed_hdf_asset,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


logger = logging.getLogger(__name__)


# ----- Naming conventions
def sentinel_to_hls_granule(granule: Sentinel2Granule) -> str:
    """Get the HLS product output name for a Sentinel-2 granule ID"""
    hls_granule = HlsGranule(
        product="HLS",
        sensor="S30",
        tile_id=granule.tile_id,
        acquisition_time=granule.acquisition_time,
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
            f"T{granule.tile_id}",
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

    provides = (CONFIG,)

    scratch_dir: Path = field(
        default_factory=lambda: Path(os.getenv("SCRATCH_DIR", "/var/scratch"))
    )
    working_dir: Path | None = None
    purge_working_dir: bool = True

    def fetch(self) -> dict[Asset[EnvConfig], EnvConfig]:
        job_id = os.getenv("AWS_BATCH_JOB_ID", "local_job")
        working_dir = self.working_dir or self.scratch_dir / job_id

        config = EnvConfig(
            job_id=job_id,
            granule_ids=os.environ["GRANULE_LIST"].split(","),
            input_bucket=os.environ["INPUT_BUCKET"],
            output_bucket=os.environ["OUTPUT_BUCKET"],
            gibs_bucket=os.environ["GIBS_OUTPUT_BUCKET"],
            ac_code=os.environ["ACCODE"],
            working_dir=working_dir,
            debug_bucket=os.getenv("DEBUG_BUCKET"),
            replace_existing=os.getenv("REPLACE_EXISTING", "false").lower() == "true",
        )

        if working_dir.exists() and self.purge_working_dir:
            logger.info(f"Deleting pre-existing {working_dir=}")
            shutil.rmtree(working_dir)

        if not config.working_dir.exists():
            logger.info(f"Creating {working_dir=}")
            working_dir.mkdir(parents=True, exist_ok=True)

        return {CONFIG: config}


# ----- Tasks from sentinel.sh
@dataclass(frozen=True, kw_only=True)
class ConsolidateGranules(MergeTask):
    """Consolidate two "twin"" Sentinel-2 granules"""

    requires_factory = lambda gid: (
        CONFIG,
        trimmed_hdf_asset(gid),
        angle_hdf_asset(gid),
    )
    provides = (
        CONSOLIDATED_SR_HDF,
        CONSOLIDATED_ANGLE_HDF,
    )

    def __post_init__(self) -> None:
        validate_command("sentinel-consolidate")
        validate_command("sentinel-consolidate-angle")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]

        sr_list = [bundle[trimmed_hdf_asset(gid)] for gid in self.granule_ids]
        angle_list = [bundle[angle_hdf_asset(gid)] for gid in self.granule_ids]

        n_granules = len(self.granule_ids)
        if len(sr_list) != len(angle_list) != n_granules:
            raise ValueError(
                f"Expected {n_granules} SR/angle outputs, but got {sr_list=} & {angle_list}"
            )

        if len(self.granule_ids) == 1:
            consolidated_sr = sr_list[0]
            consolidated_angle = angle_list[0]
        else:
            logger.info(f"Consolidating {len(self.granule_ids)} granules")

            consolidated_sr = config.working_dir / "consolidated_sr.hdf"
            consolidated_angle = config.working_dir / "consolidated_angle.hdf"

            subprocess.run(
                ["sentinel-consolidate", *sr_list, str(consolidated_sr)], check=True
            )
            subprocess.run(
                ["sentinel-consolidate-angle", *angle_list, str(consolidated_angle)],
                check=True,
            )

        return {
            CONSOLIDATED_SR_HDF: consolidated_sr,
            CONSOLIDATED_ANGLE_HDF: consolidated_angle,
        }


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

    requires = (CONSOLIDATED_SR_HDF, CONFIG)
    provides = (RESAMPLED_HDF, NBAR_INPUT_HDF)

    def __post_init__(self) -> None:
        validate_command("sentinel-create-s2at30m")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]
        sr_hdf = bundle[CONSOLIDATED_SR_HDF]

        resampled_output = config.working_dir / "resample30m.hdf"
        resampled_output_hdr = resampled_output.with_suffix(".hdf.hdr")

        subprocess.run(
            ["sentinel-create-s2at30m", str(sr_hdf), str(resampled_output)],
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

    requires = (CONFIG, NBAR_INPUT_HDF, CONSOLIDATED_ANGLE_HDF)
    provides = (NBAR_HDF,)

    def __post_init__(self) -> None:
        validate_command("sentinel-derive-nbar")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        nbar_hdf = bundle[NBAR_INPUT_HDF]
        angle_hdf = bundle[CONSOLIDATED_ANGLE_HDF]
        config = bundle[CONFIG]

        # cfactor.hdf is created as part of NBAR correction
        cfactor = config.working_dir / "cfactor.hdf"

        subprocess.run(
            ["sentinel-derive-nbar", str(nbar_hdf), str(angle_hdf), str(cfactor)],
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

    def __post_init__(self) -> None:
        validate_command("sentinel-l8-like")

    def run(self, bundle: AssetBundle) -> AssetBundle:
        nbar_hdf = bundle[NBAR_HDF]
        granule = bundle[CONFIG].sentinel_granule

        binary = shutil.which("sentinel-l8-like")
        assert binary is not None  # checked in post init
        hls_libs_share = Path(binary).parents[1] / "share" / "hls-libs"
        param_file = hls_libs_share / f"bandpass_parameter.{granule.mission}.txt"

        subprocess.run(["sentinel-l8-like", param_file, str(nbar_hdf)], check=True)

        return {FINAL_OUTPUT_HDF: nbar_hdf}


@dataclass(frozen=True, kw_only=True)
class RenameOutputs(Task):
    """
    Renames the final HDF and Angle HDF to the standard HLS S30 naming convention.
    Mirrors `set_output_names` and `mv` commands in sentinel.sh.
    """

    requires = (CONFIG, FINAL_OUTPUT_HDF, CONSOLIDATED_ANGLE_HDF)
    provides = (OUTPUT_BASE_NAME, RENAMED_HDF, RENAMED_ANGLE_HDF)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config = bundle[CONFIG]
        hdf_path = bundle[FINAL_OUTPUT_HDF]
        angle_path = bundle[CONSOLIDATED_ANGLE_HDF]

        base_name = sentinel_to_hls_granule(config.sentinel_granule)
        renamed_hdf = config.working_dir / f"{base_name}.hdf"
        renamed_angle = config.working_dir / f"{base_name}.ANGLE.hdf"

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
class ConvertToCogs(Task):
    """Converts S2 HDF outputs to COG format.

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
class CreateThumbnail(Task):
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
class CreateMetadata(Task):
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
class CreateManifest(Task):
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
class ProcessGibs(Task):
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

        manifests = Paths()
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
class ProcessVi(Task):
    """
    Generates Vegetation Indices.
    Ports: vi_generate_indices, vi_generate_metadata, vi_generate_stac_items
    """

    requires = (CONFIG, OUTPUT_BASE_NAME, SR_MANIFEST_FILE, THUMBNAIL_FILE)
    provides = (VI_DIR, VI_MANIFEST_FILE)

    def __post_init__(self) -> None:
        validate_command("vi_generate_indices")
        validate_command("vi_generate_metadata")
        validate_command("vi_generate_stac_items")

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
        THUMBNAIL_FILE,
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
