from __future__ import annotations

import logging
import sys
from pathlib import Path

from hls_nextgen_orchestration.metrics import MetricsCollector
from hls_nextgen_orchestration.pipeline import Pipeline, PipelineBuilder

from .tasks import (
    ConvertToCogs,
    CreateMetadata,
    CreateSRManifest,
    CreateThumbnail,
    DownloadPathRows,
    EnvSource,
    LocalPathRows,
    ProcessGibs,
    ProcessPathRows,
    ProcessVi,
    RunNbar,
    UploadAll,
)


def construct_pipeline(
    working_dir: Path | None = None,
    local_pathrows_dir: Path | None = None,
) -> Pipeline:
    """Create the Landsat tiling pipeline

    Parameters
    ----------
    working_dir
        Override local processing directory
    local_pathrows_dir
        If provided, assume there is a pre-downloaded and atmospherically
        corrected Landsat path/row granules in this directory.


    Returns
    -------
    Pipeline
        Constructed pipeline
    """
    pathrows_task: LocalPathRows | DownloadPathRows
    if local_pathrows_dir:
        pathrows_task = LocalPathRows(
            "LocalPathRows", local_pathrows_dir=local_pathrows_dir
        )
    else:
        pathrows_task = DownloadPathRows("DownloadPathRows")

    return (
        PipelineBuilder()
        .add(EnvSource("EnvConfig", working_dir=working_dir))
        .add(pathrows_task)
        .add(ProcessPathRows("ProcessPathRows"))
        .add(RunNbar("RunNbar"))
        .add(ConvertToCogs("ConvertToCogs"))
        .add(CreateThumbnail("CreateThumbnail"))
        .add(CreateMetadata("CreateMetadata"))
        .add(CreateSRManifest("CreateManifest"))
        .add(ProcessGibs("ProcessGibs"))
        .add(ProcessVi("ProcessVi"))
        .add(UploadAll("UploadAll"))
        .build(metrics=MetricsCollector(pipeline_dims={"workflow": "landsat-tile"}))
    )


if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)

    local_pathrows_dir = Path(_) if (_ := os.getenv("LOCAL_PATHROWS_DIR")) else None
    try:
        pipeline = construct_pipeline(local_pathrows_dir=local_pathrows_dir)
        print(pipeline)
        context = pipeline.run()
        sys.exit(context.exit_code)
    except Exception as e:
        logging.error(f"Pipeline failed initialization: {e}")
        sys.exit(1)
