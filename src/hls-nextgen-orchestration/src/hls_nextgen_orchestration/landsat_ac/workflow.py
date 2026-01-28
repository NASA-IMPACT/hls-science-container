from __future__ import annotations

import logging
import sys

from hls_nextgen_orchestration.base import Pipeline, PipelineBuilder

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
)
from .tasks import (
    AddFmaskSds,
    CheckSolarZenith,
    ConvertScanline,
    ConvertToEspa,
    ConvertToHdf,
    CreateHlsXml,
    DownloadGranule,
    EnvSource,
    ParseMetadata,
    RenameAngleBands,
    RunFmask,
    RunLaSRC,
    UploadResults,
)


def construct_pipeline() -> Pipeline:
    return (
        PipelineBuilder()
        .add(EnvSource("EnvConfig", provides=(CONFIG,)))
        .add(
            DownloadGranule(
                "Download", requires=(CONFIG,), provides=(GRANULE_DIR, MTL_FILE)
            )
        )
        .add(ParseMetadata("Metadata", requires=(CONFIG,), provides=(METADATA,)))
        .add(
            CheckSolarZenith(
                "CheckSolar", requires=(MTL_FILE,), provides=(SOLAR_VALID,)
            )
        )
        .add(RunFmask("Fmask", requires=(CONFIG, GRANULE_DIR), provides=(FMASK_BIN,)))
        .add(
            ConvertScanline(
                "Scanline", requires=(GRANULE_DIR,), provides=(SCANLINE_DONE,)
            )
        )
        .add(
            ConvertToEspa(
                "EspaConv",
                requires=(CONFIG, MTL_FILE, GRANULE_DIR, SCANLINE_DONE),
                provides=(ESPA_XML,),
            )
        )
        .add(
            RunLaSRC("LaSRC", requires=(ESPA_XML, SOLAR_VALID), provides=(LASRC_DONE,))
        )
        .add(
            RenameAngleBands(
                "RenameAngles",
                requires=(CONFIG, METADATA, GRANULE_DIR, LASRC_DONE),
                provides=(RENAMED_ANGLES,),
            )
        )
        .add(
            CreateHlsXml(
                "HlsXml",
                requires=(CONFIG, ESPA_XML, RENAMED_ANGLES),
                provides=(HLS_XML,),
            )
        )
        .add(ConvertToHdf("HdfConv", requires=(HLS_XML,), provides=(SR_HDF,)))
        .add(
            AddFmaskSds(
                "AddFmask",
                requires=(CONFIG, METADATA, SR_HDF, FMASK_BIN, MTL_FILE),
                provides=(FINAL_HDF,),
            )
        )
        .add(
            UploadResults(
                "Upload",
                requires=(CONFIG, METADATA, FINAL_HDF, GRANULE_DIR),
                provides=(UPLOAD_COMPLETE,),
            )
        )
        .build()
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        pipeline = construct_pipeline()
        print(pipeline)
        pipeline.run()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)
