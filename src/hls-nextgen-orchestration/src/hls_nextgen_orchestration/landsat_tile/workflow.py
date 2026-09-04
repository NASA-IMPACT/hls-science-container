from __future__ import annotations

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
    cleanup_working_dir: bool = True,
) -> Pipeline:
    """Create the Landsat tiling pipeline

    Parameters
    ----------
    working_dir
        Override local processing directory
    local_pathrows_dir
        If provided, assume there is a pre-downloaded and atmospherically
        corrected Landsat path/row granules in this directory.
    cleanup_working_dir
        If True (default), remove the working directory when the pipeline
        finishes, however it finishes.

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
        .build(
            metrics=MetricsCollector(pipeline_dims={"workflow": "landsat-tile"}),
            cleanup_working_dir=cleanup_working_dir,
        )
    )


if __name__ == "__main__":
    from hls_nextgen_orchestration.cli import cli

    cli(["landsat-tile"])
