from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from hls_nextgen_orchestration.base import Pipeline, PipelineBuilder

from .mapped_tasks import (
    AddS2FmaskSds,
    ApplyS2QualityMask,
    CheckSolarZenith,
    CombineS2Hdf,
    DeriveS2Angles,
    DownloadSentinelGranule,
    FindS2Footprint,
    GetGranuleDir,
    LocalSentinelGranule,
    PrepareEspaInput,
    ProcessHdfParts,
    RunFmask,
    RunLaSRC,
    TrimS2Hdf,
)
from .tasks import (
    BandpassCorrection,
    ConsolidateGranules,
    DeriveNbar,
    EnvSource,
    RenameS2Outputs,
    Resample30m,
    S2ConvertToCogs,
    S2CreateManifest,
    S2CreateMetadata,
    S2CreateThumbnail,
    S2ProcessGibs,
    S2ProcessVi,
    UploadAll,
)

logger = logging.getLogger(__name__)


# FIXME: docstring
def construct_pipeline(
    granule_ids: list[str] | None = None,
    working_dir: Path | None = None,
    local_granule_zips: list[Path] | None = None,
    upload: bool = True,
) -> Pipeline:
    """Constructs the Sentinel-2 (S30) Preprocessing Pipeline.

    Parameters
    ----------
    TODO
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
            .add(FindS2Footprint.map(granule_id)(name="FindFootprint"))
            .add(ApplyS2QualityMask.map(granule_id)(name="ApplyMask"))
            .add(DeriveS2Angles.map(granule_id)(name="DeriveAngles"))
            .add(RunFmask.map(granule_id)("Fmask"))
            .add(PrepareEspaInput.map(granule_id)("PrepareEspa"))
            .add(RunLaSRC.map(granule_id)("LaSRC"))
            .add(ProcessHdfParts.map(granule_id)("ProcessHdfParts"))
            .add(CombineS2Hdf.map(granule_id)("CombineParts"))
            .add(AddS2FmaskSds.map(granule_id)("AddFmaskSds"))
            .add(TrimS2Hdf.map(granule_id)("Trim"))
        )

    # Post-single granule workflow
    builder = (
        builder.add(ConsolidateGranules.merge(granule_ids)("ConsolidateGranules"))
        .add(Resample30m.map(granule_id)("Resample"))
        .add(DeriveNbar("Nbar"))
        .add(BandpassCorrection("Bandpass"))
        .add(RenameS2Outputs("RenameOutputs"))
        .add(S2ConvertToCogs("ConvertToCogs"))
        .add(S2CreateThumbnail("CreateThumbnail"))
        .add(S2CreateMetadata("CreateMetadata"))
        .add(S2CreateManifest("CreateManifest"))
        .add(S2ProcessGibs("ProcessGibs"))
        .add(S2ProcessVi("ProcessVi"))
    )

    if upload:
        builder = builder.add(UploadAll("Upload"))

    return builder.build()


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
    else:
        local_granule_zips = None

    try:
        pipeline = construct_pipeline(
            granule_ids=granule_ids, local_granule_zips=local_granule_zips, upload=False
        )
        print(pipeline)
        pipeline.run()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)
