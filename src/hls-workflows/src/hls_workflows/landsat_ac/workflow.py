from __future__ import annotations

from pathlib import Path

from hls_workflows.constants import FMASK_VERSION, LASRC_VERSION
from hls_workflows.metrics import MetricsCollector, MetricSink
from hls_workflows.pipeline import Pipeline, PipelineBuilder

from .lasrc_rs import RegisterLaSRC, RunLaSRCRust
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
    RunFmaskV5,
    RunLaSRC,
    UploadResults,
)


def construct_pipeline(
    granule_id: str,
    working_dir: Path | None = None,
    granule_dir: Path | None = None,
    local_granule_dir: Path | None = None,
    fmask_version: FMASK_VERSION = "v5",
    lasrc_version: LASRC_VERSION = "c",
    upload: bool = True,
    cleanup_working_dir: bool = True,
    metric_sink: MetricSink | None = None,
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
        Fmask version to use. Only "v5" is supported.
    lasrc_version
        LaSRC implementation, "c" (default) or "rust" (lasrc-rs). Selects the
        LaSRC steps of the pipeline and, with them, the recorded AC code.
    upload
        If True (default), upload to output bucket.
    cleanup_working_dir
        If True (default), remove the working directory when the pipeline
        finishes, however it finishes.

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

    fmask_task = RunFmaskV5("Fmask")

    builder = (
        PipelineBuilder()
        .add(
            EnvSource(
                "EnvConfig",
                granule_id=granule_id,
                working_dir=working_dir,
                granule_dir=granule_dir,
                lasrc_version=lasrc_version,
            )
        )
        .add(granule_task)
        .add(ParseMetadata("Metadata"))
        .add(CheckSolarZenith("CheckSolar"))
        .add(fmask_task)
        .add(ConvertScanline("Scanline"))
        .add(ConvertToEspa("EspaConv"))
    )

    if lasrc_version == "rust":
        builder = builder.add(RunLaSRCRust("LaSRC")).add(RegisterLaSRC("RegisterLaSRC"))
    else:
        builder = builder.add(RunLaSRC("LaSRC"))

    builder = (
        builder.add(RenameAngleBands("RenameAngles"))
        .add(CreateHlsXml("HlsXml"))
        .add(ConvertToHdf("HdfConv"))
        .add(AddFmaskSds("AddFmask"))
    )

    if upload:
        builder = builder.add(UploadResults("Upload"))

    return builder.build(
        metrics=MetricsCollector(
            pipeline_dims={
                "workflow": "landsat-ac",
                "lasrc_version": lasrc_version,
            },
            pipeline_props={"input_granule_id": granule_id},
            sink=metric_sink,
        ),
        cleanup_working_dir=cleanup_working_dir,
    )


if __name__ == "__main__":
    from hls_workflows.cli import cli

    cli(["landsat-ac"])
