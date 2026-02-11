from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hls_nextgen_orchestration.base import Asset
from hls_nextgen_orchestration.common.assets import ProcessingMetadata
from hls_nextgen_orchestration.granules import Sentinel2Granule


@dataclass(frozen=True)
class SentinelMetadata(ProcessingMetadata):
    """
    Sentinel-2 specific metadata.
    """

    tile_id: str
    year: str
    doy: str
    obs_index: str
    hls_ver: str


@dataclass(frozen=True, kw_only=True)
class EnvConfig:
    """
    Configuration for the Sentinel-2 processing environment.
    """

    job_id: str
    granule: str
    input_bucket: str
    output_bucket: str
    gibs_bucket: str
    working_dir: Path
    granule_dir: Path
    prefix: str
    ac_code: str
    debug_bucket: str | None = None
    replace_existing: bool = False

    @property
    def sentinel_granule(self) -> Sentinel2Granule:
        """Get the parsed SentinelGranule object."""
        return Sentinel2Granule.from_str(self.granule)


# Input Assets
CONFIG = Asset("sentinel_config", EnvConfig)
SAFE_DIR = Asset("safe_directory", Path)
SAFE_GRANULE_INNER_DIR = Asset("safe_granule_inner_dir", Path)

# Intermediate Processing Assets
SOLAR_VALID = Asset("solar_valid_flag", bool)
DETFOO_FILE = Asset("detfoo_file", Path)
QUALITY_MASK_APPLIED = Asset("quality_mask_applied", bool)
ANGLE_HDF = Asset("angle_hdf", Path)
FMASK_BIN = Asset("fmask_binary", Path)
MASKED_SAFE_ZIP = Asset("masked_safe_zip", Path)
ESPA_XML = Asset("espa_xml", Path)
LASRC_OUTPUT_DIR = Asset("lasrc_output_dir", Path)
SPLIT_HDF_PARTS = Asset("split_hdf_parts", list)
COMBINED_SR_HDF = Asset("combined_sr_hdf", Path)
FINAL_SR_HDF = Asset("final_sr_hdf", Path)  # After adding Fmask and Trimming
TRIMMED_HDF = Asset("trimmed_hdf", Path)

# Post-Processing Assets (from sentinel.sh)
RESAMPLED_HDF = Asset("resampled_30m_hdf", Path)
NBAR_INPUT_HDF = Asset("nbar_input_hdf", Path)  # Moved/Copied from Resampled
NBAR_HDF = Asset("nbar_output_hdf", Path)
FINAL_OUTPUT_HDF = Asset("final_output_hdf", Path)  # After L8Like

# Post-Processing / Upload Assets
OUTPUT_BASE_NAME = Asset("output_base_name", str)  # e.g., HLS.S30.T123...
RENAMED_HDF = Asset("renamed_output_hdf", Path)
RENAMED_ANGLE_HDF = Asset("renamed_angle_hdf", Path)
COGS_CREATED = Asset("cogs_created", bool)
THUMBNAIL_FILE = Asset("thumbnail_file", Path)
CMR_XML = Asset("cmr_xml", Path)
STAC_JSON = Asset("stac_json", Path)
SR_MANIFEST_FILE = Asset("sr_manifest_file", Path)

# GIBS & VI Assets
GIBS_DIR = Asset("gibs_dir", Path)
GIBS_MANIFEST_FILES = Asset("gibs_manifest_files", list)
VI_DIR = Asset("vi_dir", Path)
VI_MANIFEST_FILE = Asset("vi_manifest_file", Path)
UPLOAD_COMPLETE = Asset("upload_complete", bool)
