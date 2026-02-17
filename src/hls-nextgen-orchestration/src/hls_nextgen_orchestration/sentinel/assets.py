from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hls_nextgen_orchestration.base import Asset
from hls_nextgen_orchestration.common import Paths
from hls_nextgen_orchestration.granules import Sentinel2Granule


@dataclass(frozen=True, kw_only=True)
class EnvConfig:
    """
    Configuration for the Sentinel-2 processing environment.
    """

    job_id: str
    granule_ids: list[str]
    input_bucket: str
    output_bucket: str
    gibs_bucket: str
    working_dir: Path
    prefix: str
    ac_code: str
    debug_bucket: str | None = None
    replace_existing: bool = False

    @property
    def sentinel_granule(self) -> Sentinel2Granule:
        """Get the primary SentinelGranule object.

        For the twin granule scenario we still need a primary source of
        reference for things like the processing timestamp.
        """
        # FIXME: shouldn't this be deterministic? hls-sentinel script is NOT
        return Sentinel2Granule.from_str(self.granule_ids[0])


# ----- Asset definitions and factories
#  Assets
CONFIG = Asset("sentinel_config", EnvConfig)


# --- Per granule tasks
# These functions create unique Asset instances for each granule ID.
# This prevents collisions in the DAG when processing multiple granules.
def safe_dir_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"safe_dir_{granule_id}", Path)


def granule_dir_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"granule_dir_{granule_id}", Path)


def mtd_tl_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"MTD_TL_{granule_id}", Path)


def solar_valid_asset(granule_id: str) -> Asset[bool]:
    return Asset(f"solar_valid_flag_{granule_id}", bool)


def detfoo_file_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"detfoo_file_{granule_id}", Path)


def quality_mask_applied_asset(granule_id: str) -> Asset[bool]:
    return Asset(f"quality_mask_applied_{granule_id}", bool)


def angle_hdf_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"angle_hdf_{granule_id}", Path)


def fmask_bin_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"fmask_binary_{granule_id}", Path)


def masked_safe_zip_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"masked_safe_zip_{granule_id}", Path)


def espa_xml_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"espa_xml_{granule_id}", Path)


def lasrc_aerosol_qa_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"lasrc_aerosol_qa_{granule_id}", Path)


def split_hdf_parts_asset(granule_id: str) -> Asset[Paths]:
    return Asset(f"split_hdf_parts_{granule_id}", Paths)


def combined_sr_hdf_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"combined_sr_hdf_{granule_id}", Path)


def final_sr_hdf_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"final_sr_hdf_{granule_id}", Path)


def trimmed_hdf_asset(granule_id: str) -> Asset[Path]:
    return Asset(f"trimmed_hdf_{granule_id}", Path)


# --- Consolidated & Post-Processing Assets (sentinel.sh)

# These remain singletons as they occur after the merge.
CONSOLIDATED_ANGLE_HDF = Asset("consolidated_angle_hdf", Path)
CONSOLIDATED_SR_HDF = Asset("consolidated_sr_hdf", Path)
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
GIBS_MANIFEST_FILES = Asset("gibs_manifest_files", Paths)
VI_DIR = Asset("vi_dir", Path)
VI_MANIFEST_FILE = Asset("vi_manifest_file", Path)
UPLOAD_COMPLETE = Asset("upload_complete", bool)
