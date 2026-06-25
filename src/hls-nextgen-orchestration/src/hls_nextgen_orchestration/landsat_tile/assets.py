from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from hls_nextgen_orchestration.base import Asset
from hls_nextgen_orchestration.common import S3Path

# --- Data Structures ---


@dataclass(frozen=True)
class EnvConfig:
    """
    Configuration for the Landsat Tile environment variables.

    Attributes
    ----------
    job_id : str
        The AWS Batch job ID.
    pathrow_list : list[str]
        List of pathrows to process.
    date : dt.date
        Date object.
    mgrs : str
        MGRS tile ID.
    mgrs_ulx : str
        Upper left X coordinate.
    mgrs_uly : str
        Upper left Y coordinate.
    input_bucket : str
        Input S3 bucket.
    output_bucket : str
        Output S3 bucket.
    gibs_bucket : str
        GIBS output bucket.
    debug_bucket : str | None
        Optional debug bucket.
    """

    job_id: str
    pathrow_list: list[str]
    date: dt.date
    mgrs: str
    mgrs_ulx: str
    mgrs_uly: str
    input_bucket: str
    output_bucket: str
    gibs_bucket: str
    working_dir: Path
    debug_bucket: str | None = None

    def output_path(self, granule_id: str) -> S3Path:
        """Main HLS product output location for a given granule.

        Ports
        -----
        bucket_key="s3://${bucket}/L30/data/${year}${day_of_year}/${granule_id}"
        """
        key = f"L30/data/{self.date.strftime('%Y%j')}/{granule_id}"
        return S3Path(self.output_bucket, key)

    def vi_path(self, vi_output_name: str) -> S3Path:
        """HLS Vegetation Index product output location for a given granule.

        Ports
        -----
        vi_bucket_key="s3://${bucket}/L30_VI/data/${year}${day_of_year}/${vi_outputname}"
        """
        key = f"L30_VI/data/{self.date.strftime('%Y%j')}/{vi_output_name}"
        return S3Path(self.output_bucket, key)

    @property
    def gibs_path(self) -> S3Path:
        """GIBS browse output location.

        Ports
        -----
        gibs_bucket_key="s3://${gibs_bucket}/L30/data/${year}${day_of_year}"
        """
        key = f"L30/data/{self.date.strftime('%Y%j')}"
        return S3Path(self.gibs_bucket, key)


# --- Assets ---

CONFIG = Asset("config_object", EnvConfig)

# Inputs from Landsat atmospheric correction workflow
PATHROW_IMAGES = Asset("pathrow_images", dict)

# Intermediate outputs from the tiling loop
NBAR_INPUT = Asset("nbar_input_hdf", Path)
NBAR_ANGLE = Asset("nbar_angle_hdf", Path)
SCENE_TIME = Asset("scene_time_str", str)
OUTPUT_BASE_NAME = Asset("output_base_name", str)

# Outputs from NBAR
OUTPUT_HDF = Asset("final_output_hdf", Path)
ANGLE_HDF = Asset("final_angle_hdf", Path)
GRIDDED_HDF = Asset("gridded_debug_hdf", Path)

# Outputs from Product Generation
COGS_CREATED = Asset("cogs_created_flag", bool)
THUMBNAIL_FILE = Asset("thumbnail_file", Path)
CMR_XML = Asset("cmr_metadata_xml", Path)
STAC_JSON = Asset("stac_metadata_json", Path)
SR_MANIFEST_FILE = Asset("manifest_file", Path)

# GIBS and VI Outputs
GIBS_DIR = Asset("gibs_output_directory", Path)
GIBS_MANIFEST_FILES = Asset("gibs_manifest_files", list)
VI_DIR = Asset("vi_output_directory", Path)
VI_MANIFEST_FILE = Asset("vi_manifest_file", Path)

# Final Flag
UPLOAD_COMPLETE = Asset("upload_complete_flag", bool)
