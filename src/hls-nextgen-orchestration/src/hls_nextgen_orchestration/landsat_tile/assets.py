from hls_nextgen_orchestration.base import Asset

__all__ = [
    "CONFIG",
    "NBAR_INPUT",
    "NBAR_ANGLE",
    "SCENE_TIME",
    "OUTPUT_BASE_NAME",
    "OUTPUT_HDF",
    "ANGLE_HDF",
    "GRIDDED_HDF",
    "COGS_CREATED",
    "THUMBNAIL_FILE",
    "CMR_XML",
    "STAC_JSON",
    "MANIFEST_FILE",
    "GIBS_DIR",
    "VI_DIR",
    "UPLOAD_COMPLETE",
]

# --- Assets ---

CONFIG = Asset("config_object")
# Intermediate outputs from the tiling loop
NBAR_INPUT = Asset("nbar_input_hdf")
NBAR_ANGLE = Asset("nbar_angle_hdf")
SCENE_TIME = Asset("scene_time_str")
OUTPUT_BASE_NAME = Asset("output_base_name")

# Outputs from NBAR
OUTPUT_HDF = Asset("final_output_hdf")
ANGLE_HDF = Asset("final_angle_hdf")
GRIDDED_HDF = Asset("gridded_debug_hdf")

# Outputs from Product Generation
COGS_CREATED = Asset("cogs_created_flag")
THUMBNAIL_FILE = Asset("thumbnail_file")
CMR_XML = Asset("cmr_metadata_xml")
STAC_JSON = Asset("stac_metadata_json")
MANIFEST_FILE = Asset("manifest_file")

# GIBS and VI Outputs
GIBS_DIR = Asset("gibs_output_directory")
VI_DIR = Asset("vi_output_directory")

# Final Flag
UPLOAD_COMPLETE = Asset("upload_complete_flag")
