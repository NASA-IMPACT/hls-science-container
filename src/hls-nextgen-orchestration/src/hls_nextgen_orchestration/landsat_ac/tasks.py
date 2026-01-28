import datetime as dt
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3

from hls_nextgen_orchestration.base import Asset, DataSource, Task, TaskFailure
from hls_nextgen_orchestration.utils import validate_command

from .assets import (
    CONFIG,
    ESPA_XML,
    FINAL_HDF,
    FMASK_BIN,
    GRANULE_DIR,
    HLS_XML,
    LASRC_DONE,
    METADATA,
    MTL_FILE,
    RENAMED_ANGLES,
    SCANLINE_DONE,
    SOLAR_VALID,
    SR_HDF,
    UPLOAD_COMPLETE,
    EnvConfig,
    ProcessingMetadata,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

# --- Data Source ---


@dataclass(frozen=True)
class EnvSource(DataSource):
    def fetch(self) -> dict[Asset[EnvConfig], EnvConfig]:
        config = EnvConfig(
            job_id=os.environ.get("AWS_BATCH_JOB_ID", "local_job"),
            granule=os.environ["GRANULE"],
            input_bucket=os.environ["INPUT_BUCKET"],
            output_bucket=os.environ["OUTPUT_BUCKET"],
            prefix=os.environ["PREFIX"],
            ac_code=os.environ["ACCODE"],
            debug_bucket=os.environ.get("DEBUG_BUCKET"),
        )
        if not config.granule_dir.exists():
            config.granule_dir.mkdir(parents=True, exist_ok=True)
        return {CONFIG: config}


# --- Tasks ---


@dataclass(frozen=True)
class DownloadGranule(Task):
    def __post_init__(self) -> None:
        validate_command("download_landsat")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        logging.info(f"Downloading {config.granule} from {config.input_bucket}...")
        subprocess.run(
            [
                "download_landsat",
                config.input_bucket,
                config.prefix,
                str(config.granule_dir),
            ],
            check=True,
        )
        mtl_path = config.granule_dir / f"{config.granule}_MTL.txt"
        if not mtl_path.exists():
            raise RuntimeError(f"Output file missing: {mtl_path}")
        return {GRANULE_DIR: config.granule_dir, MTL_FILE: mtl_path}


@dataclass(frozen=True)
class ParseMetadata(Task):
    """Parse metadata, determining output filename and prefix"""

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]

        # Use the Dataclass property from EnvConfig
        granule = config.landsat_granule

        # Construct output names using the parsed object
        # Format: YYYY-MM-DD_PPPRRR
        date_str = granule.acquisition_date.strftime("%Y-%m-%d")
        output_name = f"{date_str}_{granule.path_row}"

        # Bucket key: YYYY-MM-DD/PPPRRR
        bucket_key = f"{date_str}/{granule.path_row}"

        return {
            METADATA: ProcessingMetadata(
                output_name=output_name,
                bucket_key=bucket_key,
            )
        }


@dataclass(frozen=True)
class CheckSolarZenith(Task):
    def __post_init__(self) -> None:
        validate_command("check_solar_zenith_landsat")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        mtl_path: Path = inputs[MTL_FILE]
        logging.info("Checking Solar Zenith...")
        result = subprocess.run(
            ["check_solar_zenith_landsat", str(mtl_path)],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.stdout.strip() == "invalid":
            raise TaskFailure("Invalid solar zenith angle", exit_code=3)

        return {SOLAR_VALID: True}


@dataclass(frozen=True)
class RunFmask(Task):
    def __post_init__(self) -> None:
        validate_command("run_Fmask.sh")
        validate_command("gdal_translate")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        granule_dir: Path = inputs[GRANULE_DIR]
        cwd = os.getcwd()
        os.chdir(granule_dir)
        fmask_bin_path = granule_dir / "fmask.bin"
        try:
            logging.info("Running Fmask...")
            with open("fmask_out.txt", "a") as outfile:
                subprocess.run(["run_Fmask.sh"], stdout=outfile, check=True)
            fmask_tif = f"{config.granule}_Fmask4.tif"
            fmask_bin = "fmask.bin"
            logging.info("Converting Fmask to ENVI binary...")
            subprocess.run(
                ["gdal_translate", "-of", "ENVI", fmask_tif, fmask_bin], check=True
            )
        finally:
            os.chdir(cwd)
        if not fmask_bin_path.exists():
            raise RuntimeError(f"Output file missing: {fmask_bin_path}")
        return {FMASK_BIN: fmask_bin_path}


@dataclass(frozen=True)
class ConvertScanline(Task):
    def __post_init__(self) -> None:
        validate_command("gdal_translate")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        granule_dir: Path = inputs[GRANULE_DIR]
        tifs = list(granule_dir.glob("*.TIF"))
        logging.info(f"Converting {len(tifs)} TIFs to scanline...")
        for f in tifs:
            scan_name = f.with_name(f"{f.stem}_scan.tif")
            subprocess.run(
                ["gdal_translate", "-co", "TILED=NO", str(f), str(scan_name)],
                check=True,
            )
            f.unlink()
            scan_name.rename(f)
            if not f.exists():
                raise RuntimeError(f"Failed to create scanline file: {f}")
        return {SCANLINE_DONE: True}


@dataclass(frozen=True)
class ConvertToEspa(Task):
    def __post_init__(self) -> None:
        validate_command("convert_lpgs_to_espa")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        _ = inputs[SCANLINE_DONE]
        config: EnvConfig = inputs[CONFIG]
        mtl_path: Path = inputs[MTL_FILE]
        granule_dir: Path = inputs[GRANULE_DIR]
        espa_xml_path = granule_dir / f"{config.granule}.xml"
        cwd = os.getcwd()
        os.chdir(granule_dir)
        try:
            logging.info("Convert to ESPA")
            subprocess.run(["convert_lpgs_to_espa", f"--mtl={mtl_path}"], check=True)
        finally:
            os.chdir(cwd)
        if not espa_xml_path.exists():
            raise RuntimeError(f"Output file missing: {espa_xml_path}")
        return {ESPA_XML: espa_xml_path}


@dataclass(frozen=True)
class RunLaSRC(Task):
    def __post_init__(self) -> None:
        validate_command("do_lasrc_landsat.py")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        _ = inputs[SOLAR_VALID]
        xml_file: Path = inputs[ESPA_XML]
        granule_dir = xml_file.parent
        cwd = os.getcwd()
        os.chdir(granule_dir)
        try:
            logging.info("Run lasrc")
            subprocess.run(["do_lasrc_landsat.py", "--xml", str(xml_file)], check=True)
        finally:
            os.chdir(cwd)
        return {LASRC_DONE: True}


@dataclass(frozen=True)
class RenameAngleBands(Task):
    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        _ = inputs[LASRC_DONE]
        config: EnvConfig = inputs[CONFIG]
        meta = inputs[METADATA]
        granule_dir: Path = inputs[GRANULE_DIR]
        old_base = config.granule
        new_base = meta.output_name
        logging.info("Rename angle bands")
        suffixes = ["_VAA", "_VZA", "_SAA", "_SZA"]
        extensions = [".hdr", ".img"]
        for suffix in suffixes:
            for ext in extensions:
                old = granule_dir / f"{old_base}{suffix}{ext}"
                new = granule_dir / f"{new_base}{suffix}{ext}"
                if old.exists():
                    old.rename(new)
                else:
                    logging.warning(f"File {old} not found for renaming")
        return {RENAMED_ANGLES: True}


@dataclass(frozen=True)
class CreateHlsXml(Task):
    def __post_init__(self) -> None:
        validate_command("create_landsat_sr_hdf_xml")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        _ = inputs[RENAMED_ANGLES]
        config: EnvConfig = inputs[CONFIG]
        espa_xml: Path = inputs[ESPA_XML]
        granule_dir = espa_xml.parent
        hls_xml = granule_dir / f"{config.granule}_hls.xml"
        cwd = os.getcwd()
        os.chdir(granule_dir)
        try:
            logging.info("Create updated espa xml")
            subprocess.run(
                ["create_landsat_sr_hdf_xml", str(espa_xml), str(hls_xml)], check=True
            )
        finally:
            os.chdir(cwd)
        if not hls_xml.exists():
            raise RuntimeError(f"Output file missing: {hls_xml}")
        return {HLS_XML: hls_xml}


@dataclass(frozen=True)
class ConvertToHdf(Task):
    def __post_init__(self) -> None:
        validate_command("convert_espa_to_hdf")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        xml: Path = inputs[HLS_XML]
        granule_dir = xml.parent
        sr_hdf = granule_dir / "sr.hdf"
        cwd = os.getcwd()
        os.chdir(granule_dir)
        try:
            logging.info("Convert to HDF")
            subprocess.run(
                ["convert_espa_to_hdf", f"--xml={xml}", f"--hdf={sr_hdf}"], check=True
            )
        finally:
            os.chdir(cwd)
        if not sr_hdf.exists():
            raise RuntimeError(f"Output file missing: {sr_hdf}")
        return {SR_HDF: sr_hdf}


@dataclass(frozen=True)
class AddFmaskSds(Task):
    def __post_init__(self) -> None:
        validate_command("landsat-add-fmask-sds")

    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        meta = inputs[METADATA]
        sr_hdf: Path = inputs[SR_HDF]
        fmask_bin: Path = inputs[FMASK_BIN]
        mtl: Path = inputs[MTL_FILE]
        granule_dir = sr_hdf.parent
        output_hdf = granule_dir / f"{meta.output_name}.hdf"
        aerosol_qa = granule_dir / f"{config.granule}_sr_aerosol_qa.img"
        logging.info("Run addFmaskSDS")
        subprocess.run(
            [
                "landsat-add-fmask-sds",
                str(sr_hdf),
                str(fmask_bin),
                str(aerosol_qa),
                str(mtl),
                config.ac_code,
                str(output_hdf),
            ],
            check=True,
        )
        if not output_hdf.exists():
            raise RuntimeError(f"Output file missing: {output_hdf}")
        return {FINAL_HDF: output_hdf}


@dataclass(frozen=True)
class UploadResults(Task):
    def run(self, inputs: dict[Any, Any]) -> dict[Any, Any]:
        config: EnvConfig = inputs[CONFIG]
        meta = inputs[METADATA]
        final_hdf: Path = inputs[FINAL_HDF]
        granule_dir: Path = inputs[GRANULE_DIR]
        s3: S3Client = boto3.client("s3")
        if not config.debug_bucket:
            bucket = config.output_bucket
            bucket_key = meta.bucket_key
            hdf_key = f"{bucket_key}/{final_hdf.name}"
            logging.info(f"Uploading {final_hdf.name} to s3://{bucket}/{hdf_key}")
            s3.upload_file(str(final_hdf), bucket, hdf_key)
            logging.info("Uploading angle files...")
            include_globs = [
                "*_VAA.img",
                "*_VAA.hdr",
                "*_VZA.hdr",
                "*_VZA.img",
                "*_SAA.hdr",
                "*_SAA.img",
                "*_SZA.hdr",
                "*_SZA.img",
            ]
            for pattern in include_globs:
                for f in granule_dir.glob(pattern):
                    key = f"{bucket_key}/{f.name}\n"
                    s3.upload_file(str(f), bucket, key)
        else:
            timestamp = dt.datetime.now().strftime("%Y_%m_%d_%H_%M")
            bucket = config.debug_bucket
            base_key = f"{config.granule}_{timestamp}"
            logging.info("Copy files to debug bucket")
            for f in granule_dir.rglob("*"):
                if f.is_file():
                    rel_path = f.relative_to(granule_dir)
                    key = f"{base_key}/{rel_path}"
                    s3.upload_file(str(f), bucket, key)
        return {UPLOAD_COMPLETE: True}
