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
    DeriveNbar,
    DeriveS2Angles,
    DownloadSentinelGranule,
    EnvSource,
    FindS2Footprint,
    GetS2GranuleDir,
    LocalSentinelGranule,
    PrepareEspaInput,
    ProcessHdfParts,
    RenameS2Outputs,
    Resample30m,
    RunS2Fmask,
    RunS2LaSRC,
    S2ConvertToCogs,
    S2CreateManifest,
    S2CreateMetadata,
    S2CreateThumbnail,
    S2ProcessGibs,
    S2ProcessVi,
    TrimS2Hdf,
    UploadAll,
)


def construct_pipeline(
    working_dir: Path | None = None,
    local_granule_zip: Path | None = None,
    upload: bool = True,
) -> Pipeline:
    """
    Constructs the Sentinel-2 (S30) Preprocessing Pipeline.
    Combines logic from sentinel_granule.sh (AC) and sentinel.sh (Resample/NBAR).
    """
    granule_task: LocalSentinelGranule | DownloadSentinelGranule
    if local_granule_zip:
        granule_task = LocalSentinelGranule(
            "LocalGranule", local_granule_zip=local_granule_zip
        )
    else:
        granule_task = DownloadSentinelGranule("Download")

    builder = (
        PipelineBuilder()
        .add(EnvSource("EnvConfig", working_dir=working_dir))
        .add(granule_task)
        .add(GetS2GranuleDir("GetInnerDir"))
        .add(CheckSolarZenith("CheckSolar"))
        .add(FindS2Footprint(name="FindFootprint"))
        .add(ApplyS2QualityMask(name="ApplyMask"))
        .add(DeriveS2Angles(name="DeriveAngles"))
        .add(RunS2Fmask("Fmask"))
        .add(PrepareEspaInput("PrepareEspa"))
        .add(RunS2LaSRC("LaSRC"))
        .add(ProcessHdfParts("ProcessHdfParts"))
        .add(CombineS2Hdf("CombineParts"))
        .add(AddS2FmaskSds("AddFmaskSds"))
        .add(TrimS2Hdf("Trim"))
        .add(Resample30m("Resample"))
        .add(DeriveNbar("Nbar"))
        .add(BandpassCorrection("Bandpass"))
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
