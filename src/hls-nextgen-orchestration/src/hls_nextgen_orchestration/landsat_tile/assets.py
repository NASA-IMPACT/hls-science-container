from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from hls_nextgen_orchestration.base import Asset

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
    gcc_role_arn: str | None = None

    @property
    def year(self) -> str:
        """Get the year from the date."""
        return str(self.date.year)

    @property
    def day_of_year(self) -> str:
        """Get the day of year from the date."""
        return self.date.strftime("%j")


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
