from __future__ import annotations

import logging
import sys
from pathlib import Path

from hls_nextgen_orchestration.base import Pipeline, PipelineBuilder

from .tasks import (
    AddS2FmaskSds,
    ApplyS2QualityMask,
    BandpassCorrection,
    CheckSolarZenith,
    CombineS2Hdf,
    ConsolidateGranules,
    DeriveNbar,
    DeriveS2Angles,
    DownloadSentinelGranule,
    EnvSource,
    FindS2Footprint,
    GetGranuleDir,
    LocalSentinelGranule,
    PrepareEspaInput,
    ProcessHdfParts,
    RenameS2Outputs,
    Resample30m,
    RunFmask,
    RunLaSRC,
    S2ConvertToCogs,
    S2CreateManifest,
    S2CreateMetadata,
    S2CreateThumbnail,
    S2ProcessGibs,
    S2ProcessVi,
    TrimS2Hdf,
    UploadAll,
)

logger = logging.getLogger(__name__)


# FIXME: docstring
def construct_pipeline(
    granule_list: list[str] = None,
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
    if granule_list:
        logger.info("Using granule list from function input...")
    elif granule_list_str := os.getenv("GRANULE_LIST"):
        logger.info("")
        granule_list = granule_list_str.split(",")
    else:
        raise ValueError("Must define input granules as input or envvar.")

    # If exists, validate and form pairing of IDs to local granules
    if local_granule_zips:
        if len(local_granule_zips) != len(granule_list):
            raise ValueError("Mismatch in number of granules to local ZIP paths")
        local_granule_ids_to_zips = dict(zip(granule_list, local_granule_zips))

    # Build pipeline based on granules
    builder = PipelineBuilder().add(EnvSource("EnvConfig", working_dir=working_dir))

    # Map each granule to all steps in sub-workflow sequentially
    for granule_id in granule_list:
        granule_zip_task: LocalSentinelGranule | DownloadSentinelGranule
        if local_granule_ids_to_zips:
            granule_zip_task = LocalSentinelGranule.map(granule_id)(
                "LocalGranule", local_granule_zip=local_granule_ids_to_zips[granule_id],
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
            .add(Resample30m.map(granule_id)("Resample"))
            .add(DeriveNbar.map(granule_id)("Nbar"))
            .add(BandpassCorrection.map(granule_id)("Bandpass"))
        )

    # FIXME: don't consolidate if just 1 granule

    # P
    builder = (
        buider
        # Post-Processing
        .add(RenameS2Outputs(name="RenameOutputs"))
        .add(S2ConvertToCogs(name="ConvertToCogs"))
        .add(S2CreateThumbnail(name="CreateThumbnail"))
        .add(S2CreateMetadata(name="CreateMetadata"))
        .add(S2CreateManifest(name="CreateManifest"))
        .add(S2ProcessGibs(name="ProcessGibs"))
        .add(S2ProcessVi(name="ProcessVi"))
    )

    if upload:
        builder = builder.add(UploadAll("Upload"))

    return builder.build()


if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)

    local_granule_zip = Path(_) if (_ := os.getenv("LOCAL_GRANULE_ZIP")) else None

    try:
        pipeline = construct_pipeline(local_granule_zip=local_granule_zip, upload=False)
        print(pipeline)
        pipeline.run()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)
