from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from hls_nextgen_orchestration.constants import FMASK_VERSION
from hls_nextgen_orchestration.metrics import MetricsCollector
from hls_nextgen_orchestration.pipeline import Pipeline, PipelineBuilder

from .mapped_tasks import (
    AddFmaskSds,
    ApplyQualityMask,
    CheckSolarZenith,
    CombineHdf,
    DeriveAngles,
    DownloadSentinelGranule,
    FindFootprint,
    GetGranuleDir,
    LocalSentinelGranule,
    PrepareEspaInput,
    ProcessHdfParts,
    RunFmask,
    RunFmaskV5,
    RunLaSRC,
    TrimHdf,
)
from .tasks import (
    BandpassCorrection,
    ConsolidateGranules,
    ConvertToCogs,
    CreateManifest,
    CreateMetadata,
    CreateThumbnail,
    DeriveNbar,
    EnvSource,
    ProcessGibs,
    ProcessVi,
    RenameOutputs,
    Resample30m,
    UploadAll,
)

logger = logging.getLogger(__name__)


def construct_pipeline(
    granule_ids: list[str] | None = None,
    working_dir: Path | None = None,
    local_granule_zips: list[Path] | None = None,
    fmask_version: FMASK_VERSION = "v4",
    upload: bool = True,
) -> Pipeline:
    """Constructs the Sentinel-2 (S30) Preprocessing Pipeline.

    Parameters
    ----------
    granule_ids
        Granule ID(s) to process. Usually this will be just 1, except
        for twin granule cases. We need to know this ahead of time to
        build the pipeline correctly. If not provided, the `GRANULE_LIST`
        environment variable must be defined.
    working_dir
        Override local processing directory
    local_granule_zips
        If provided, assume there is a pre-downloaded Sentinel-2 granule(s)
        to process in this directory.
    fmask_version
        Fmask version to use: "v4" (default) or "v5".
    upload
        If True (default), upload to output bucket.
    """
    # Parse input granule list
    if granule_ids:
        logger.info("Using granule list from function input...")
    elif granule_list_str := os.getenv("GRANULE_LIST"):
        logger.info("")
        granule_ids = granule_list_str.split(",")
    else:
        raise ValueError("Must define input granules as input or envvar.")

    # If exists, validate and form pairing of IDs to local granules
    if local_granule_zips:
        if len(local_granule_zips) != len(granule_ids):
            raise ValueError("Mismatch in number of granules to local ZIP paths")
        local_granule_ids_to_zips = dict(zip(granule_ids, local_granule_zips))
    else:
        local_granule_ids_to_zips = {}

    # Build pipeline based on granules
    builder = PipelineBuilder().add(EnvSource("EnvConfig", working_dir=working_dir))

    # Map each granule to all steps in sub-workflow sequentially
    for granule_id in granule_ids:
        # Download granule only if path to it hasn't been provided
        granule_zip_task: LocalSentinelGranule | DownloadSentinelGranule
        if local_granule_ids_to_zips:
            granule_zip_task = LocalSentinelGranule.map(granule_id)(
                "LocalGranule",
                local_granule_zip=local_granule_ids_to_zips[granule_id],
            )
        else:
            granule_zip_task = DownloadSentinelGranule.map(granule_id)("Download")

        builder = (
            builder
            # Per-granule tasks
            .add(granule_zip_task)
            .add(GetGranuleDir.map(granule_id)("GetInnerDir"))
            .add(CheckSolarZenith.map(granule_id)("CheckSolar"))
            .add(FindFootprint.map(granule_id)(name="FindFootprint"))
            .add(ApplyQualityMask.map(granule_id)(name="ApplyMask"))
            .add(DeriveAngles.map(granule_id)(name="DeriveAngles"))
            .add(
                RunFmaskV5.map(granule_id)("Fmask")
                if fmask_version == "v5"
                else RunFmask.map(granule_id)("Fmask")
            )
            .add(PrepareEspaInput.map(granule_id)("PrepareEspa"))
            .add(RunLaSRC.map(granule_id)("LaSRC"))
            .add(ProcessHdfParts.map(granule_id)("ProcessHdfParts"))
            .add(CombineHdf.map(granule_id)("CombineParts"))
            .add(AddFmaskSds.map(granule_id)("AddFmaskSds"))
            .add(TrimHdf.map(granule_id)("Trim"))
        )

    # Post-single granule workflow
    builder = (
        builder.add(ConsolidateGranules.merge(granule_ids)("ConsolidateGranules"))
        .add(Resample30m("Resample"))
        .add(DeriveNbar("Nbar"))
        .add(BandpassCorrection("Bandpass"))
        .add(RenameOutputs("RenameOutputs"))
        .add(ConvertToCogs("ConvertToCogs"))
        .add(CreateThumbnail("CreateThumbnail"))
        .add(CreateMetadata("CreateMetadata"))
        .add(CreateManifest("CreateManifest"))
        .add(ProcessGibs("ProcessGibs"))
        .add(ProcessVi("ProcessVi"))
    )

    if upload:
        builder = builder.add(UploadAll("Upload"))

    return builder.build(
        metrics=MetricsCollector(pipeline_dims={"workflow": "sentinel-ac"})
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    granule_ids: list[str] | None
    if env := os.getenv("GRANULE_LIST"):
        granule_ids = env.split(",")
    else:
        granule_ids = None

    local_granule_zips: list[Path] | None
    if env := os.getenv("LOCAL_GRANULE_ZIPS"):
        local_granule_zips = [Path(zf) for zf in env.split(",")]
        granule_ids = [granule_zip.stem for granule_zip in local_granule_zips]
        os.environ["GRANULE_LIST"] = ",".join(granule_ids)
    else:
        local_granule_zips = None

    fmask_version: FMASK_VERSION = "v5" if os.getenv("FMASK_VERSION") == "5" else "v4"

    try:
        pipeline = construct_pipeline(
            granule_ids=granule_ids,
            local_granule_zips=local_granule_zips,
            fmask_version=fmask_version,
        )
        print(pipeline)
        pipeline.run()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)
