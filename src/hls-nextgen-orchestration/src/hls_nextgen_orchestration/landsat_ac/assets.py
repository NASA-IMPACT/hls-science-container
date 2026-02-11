from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hls_nextgen_orchestration.base import Asset
from hls_nextgen_orchestration.common.assets import ProcessingMetadata
from hls_nextgen_orchestration.granules import LandsatGranule


@dataclass(frozen=True)
class EnvConfig:
    """
    Configuration for the Landsat AC environment variables.
    Moved here to avoid circular imports and allow Asset typing.

    Attributes
    ----------
    job_id : str
        The AWS Batch job ID.
    granule : str
        The input Landsat granule ID.
    input_bucket : str
        S3 bucket containing input data.
    output_bucket : str
        S3 bucket for output data.
    prefix : str
        S3 prefix for input data.
    ac_code : str
        Atmospheric correction code path/params.
    working_dir : Path
        Local processing directory
    granule_dir : Path
        Local, granule-specific processing directory
    debug_bucket : str | None
        Optional S3 bucket for debug outputs.
    """

    job_id: str
    granule: str
    input_bucket: str
    output_bucket: str
    prefix: str
    ac_code: str
    working_dir: Path
    granule_dir: Path
    debug_bucket: str | None = None

    @property
    def landsat_granule(self) -> LandsatGranule:
        """Get the parsed LandsatGranule object."""
        return LandsatGranule.from_str(self.granule)


CONFIG = Asset("config_object", EnvConfig)
GRANULE_DIR = Asset("granule_directory", Path)
MTL_FILE = Asset("mtl_file", Path)
METADATA = Asset("metadata", ProcessingMetadata)
FMASK_BIN = Asset("fmask_binary", Path)
SCANLINE_DONE = Asset("scanline_converted_flag", bool)
ESPA_XML = Asset("espa_xml", Path)
LASRC_DONE = Asset("lasrc_done_flag", bool)
RENAMED_ANGLES = Asset("renamed_angles_flag", bool)
HLS_XML = Asset("hls_xml", Path)
SR_HDF = Asset("sr_hdf", Path)
FINAL_HDF = Asset("final_hdf", Path)
UPLOAD_COMPLETE = Asset("upload_complete_flag", bool)
SOLAR_VALID = Asset("solar_zenith_valid_flag", bool)
