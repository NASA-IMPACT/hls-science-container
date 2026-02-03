from __future__ import annotations

import logging
import sys
from pathlib import Path

from hls_nextgen_orchestration.base import Pipeline, PipelineBuilder

from .tasks import (
    AddFmaskSds,
    CheckSolarZenith,
    ConvertScanline,
    ConvertToEspa,
    ConvertToHdf,
    CreateHlsXml,
    DownloadGranule,
    EnvSource,
    LocalGranule,
    ParseMetadata,
    RenameAngleBands,
    RunFmask,
    RunLaSRC,
    UploadResults,
)


def construct_pipeline(
    working_dir: Path | None = None,
    granule_dir: Path | None = None,
    local_granule_dir: Path | None = None,
) -> Pipeline:
    granule_task: LocalGranule | DownloadGranule
    if local_granule_dir:
        granule_task = LocalGranule("LocalGranule", local_granule_dir=local_granule_dir)
    else:
        granule_task = DownloadGranule("Download")

    return (
        PipelineBuilder()
        .add(EnvSource("EnvConfig", working_dir=working_dir, granule_dir=granule_dir))
        .add(granule_task)
        .add(ParseMetadata("Metadata"))
        .add(CheckSolarZenith("CheckSolar"))
        .add(RunFmask("Fmask"))
        .add(ConvertScanline("Scanline"))
        .add(ConvertToEspa("EspaConv"))
        .add(RunLaSRC("LaSRC"))
        .add(RenameAngleBands("RenameAngles"))
        .add(CreateHlsXml("HlsXml"))
        .add(ConvertToHdf("HdfConv"))
        .add(AddFmaskSds("AddFmask"))
        .add(UploadResults("Upload"))
        .build()
    )


if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)

    local_granule_dir = Path(_) if (_ := os.getenv("LOCAL_GRANULE_DIR")) else None

    try:
        pipeline = construct_pipeline(local_granule_dir=local_granule_dir)
        print(pipeline)
        pipeline.run()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)
