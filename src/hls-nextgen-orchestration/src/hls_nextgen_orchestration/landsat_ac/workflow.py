from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from hls_nextgen_orchestration.constants import FMASK_VERSION
from hls_nextgen_orchestration.metrics import MetricsCollector
from hls_nextgen_orchestration.pipeline import Pipeline, PipelineBuilder

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
    RunFmaskV5,
    RunLaSRC,
    UploadResults,
)


def construct_pipeline(
    granule_id: str,
    working_dir: Path | None = None,
    granule_dir: Path | None = None,
    local_granule_dir: Path | None = None,
    fmask_version: FMASK_VERSION = "v4",
    upload: bool = True,
) -> Pipeline:
    """Create the Landsat atmospheric correction (AC) pipeline

    Parameters
    ----------
    granule_id
        Input Landsat granule ID.
    working_dir
        Override local processing directory
    granule_dir
        Override local granule-specific processing directory
    local_granule_dir
        If provided, assume there is a pre-downloaded Landsat granule
        to process in this directory.
    fmask_version
        Fmask version to use: "v4" (default) or "v5".
    upload
        If True (default), upload to output bucket.

    Returns
    -------
    Pipeline
        Constructed pipeline
    """
    granule_task: LocalGranule | DownloadGranule
    if local_granule_dir:
        granule_task = LocalGranule("LocalGranule", local_granule_dir=local_granule_dir)
    else:
        granule_task = DownloadGranule("DownloadGranule")

    fmask_task: RunFmask | RunFmaskV5
    if fmask_version == "v5":
        fmask_task = RunFmaskV5("Fmask")
    else:
        fmask_task = RunFmask("Fmask")

    builder = (
        PipelineBuilder()
        .add(
            EnvSource(
                "EnvConfig",
                granule_id=granule_id,
                working_dir=working_dir,
                granule_dir=granule_dir,
            )
        )
        .add(granule_task)
        .add(ParseMetadata("Metadata"))
        .add(CheckSolarZenith("CheckSolar"))
        .add(fmask_task)
        .add(ConvertScanline("Scanline"))
        .add(ConvertToEspa("EspaConv"))
        .add(RunLaSRC("LaSRC"))
        .add(RenameAngleBands("RenameAngles"))
        .add(CreateHlsXml("HlsXml"))
        .add(ConvertToHdf("HdfConv"))
        .add(AddFmaskSds("AddFmask"))
    )

    if upload:
        builder = builder.add(UploadResults("Upload"))

    return builder.build(
        metrics=MetricsCollector(
            pipeline_dims={"workflow": "landsat-ac", "input_granule_id": granule_id}
        )
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    granule_id = os.environ["GRANULE"]
    local_granule_dir = Path(_) if (_ := os.getenv("LOCAL_GRANULE_DIR")) else None
    fmask_version: FMASK_VERSION = "v5" if os.getenv("FMASK_VERSION") == "5" else "v4"

    try:
        pipeline = construct_pipeline(
            granule_id=granule_id,
            local_granule_dir=local_granule_dir,
            fmask_version=fmask_version,
        )
        print(pipeline)
        pipeline.run()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)
