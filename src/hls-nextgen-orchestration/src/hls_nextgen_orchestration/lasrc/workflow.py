"""Construct the standalone "just-LaSRC" pipeline.

One unified ``construct_pipeline`` detects whether the granule is Sentinel-2 or
Landsat and assembles the minimal chain needed to reach LaSRC, reusing the
existing per-mission tasks:

- Rust: ``Download -> RunLaSRCRust -> UploadLaSRCDebug`` (runs straight off the
  raw scene directory).
- C: ``Download -> ... -> RunLaSRC -> UploadLaSRCDebug``, where ``...`` is the
  ESPA-conversion chain the C LaSRC requires.

The C path does NOT run Fmask. LaSRC never consumes Fmask output; in the full pipelines Fmask is pulled into the chain
only as an *ordering* hack. The ``ConvertScanline`` & ``PrepareEspaInput`` tasks declare a fake ``FMASK_BIN``
requirement so Fmask runs before the granule is converted/repackaged). With no Fmask here, that ordering is
unnecessary, so this pipeline uses thin variants that drop the requirement.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from hls_nextgen_orchestration.constants import LASRC_VERSION
from hls_nextgen_orchestration.granules import LandsatGranule, Sentinel2Granule
from hls_nextgen_orchestration.landsat_ac import tasks as l_tasks
from hls_nextgen_orchestration.metrics import MetricsCollector, MetricSink
from hls_nextgen_orchestration.pipeline import Pipeline, PipelineBuilder
from hls_nextgen_orchestration.sentinel import mapped_tasks as s2_mapped
from hls_nextgen_orchestration.sentinel import tasks as s2_tasks

from . import landsat as l_lasrc
from . import sentinel as s2_lasrc

logger = logging.getLogger(__name__)


def _is_sentinel(granule_id: str) -> bool:
    """Detect Sentinel-2 vs Landsat from a granule ID.

    Tries the Sentinel-2 parser first, then the Landsat parser, raising a clear
    error if neither matches.
    """
    try:
        Sentinel2Granule.from_str(granule_id)
        return True
    except ValueError:
        pass
    try:
        LandsatGranule.from_str(granule_id)
        return False
    except ValueError:
        pass
    raise ValueError(
        f"Could not identify {granule_id!r} as a Sentinel-2 or Landsat granule ID"
    )


def construct_pipeline(
    granule_id: str,
    *,
    lasrc_version: LASRC_VERSION = "c",
    working_dir: Path | None = None,
    local_granule: Path | None = None,
    metric_sink: MetricSink | None = None,
) -> Pipeline:
    """Construct the standalone LaSRC (C or Rust) pipeline for one granule.

    Parameters
    ----------
    granule_id
        Sentinel-2 or Landsat granule ID (auto-detected).
    lasrc_version
        "c" (default, espa-surface-reflectance) or "rust" (lasrc-rs).
    working_dir
        Override local processing directory.
    local_granule
        Pre-downloaded granule path (a .zip for Sentinel-2, a directory for
        Landsat). If omitted, the granule is downloaded from S3.
    metric_sink
        Optional metrics sink.
    """
    builder = (
        _build_sentinel(granule_id, lasrc_version, working_dir, local_granule)
        if _is_sentinel(granule_id)
        else _build_landsat(granule_id, lasrc_version, working_dir, local_granule)
    )

    return builder.build(
        metrics=MetricsCollector(
            pipeline_dims={
                "workflow": "lasrc",
                "lasrc_version": lasrc_version,
                "input_granule_id": granule_id,
            },
            sink=metric_sink,
        )
    )


def _build_sentinel(
    granule_id: str,
    lasrc_version: LASRC_VERSION,
    working_dir: Path | None,
    local_granule: Path | None,
) -> PipelineBuilder:
    """Sentinel-2 just-LaSRC builder (single granule, no twin consolidation)."""
    # The Sentinel EnvSource reads GRANULE_LIST from the environment.
    os.environ["GRANULE_LIST"] = granule_id

    download: s2_mapped.LocalSentinelGranule | s2_mapped.DownloadSentinelGranule
    if local_granule:
        download = s2_mapped.LocalSentinelGranule.map(granule_id)(
            "LocalGranule", local_granule_zip=local_granule
        )
    else:
        download = s2_mapped.DownloadSentinelGranule.map(granule_id)("Download")

    builder = (
        PipelineBuilder()
        .add(s2_tasks.EnvSource("EnvConfig", working_dir=working_dir))
        .add(download)
    )

    if lasrc_version == "rust":
        builder = builder.add(s2_lasrc.RunLaSRCRust.map(granule_id)("LaSRC"))
    else:
        # All of these are genuine data dependencies of the angle HDF that the C
        # LaSRC needs (DeriveAngles -> FindFootprint/ApplyQualityMask). Only
        # Fmask is omitted; the ESPA prep variant drops its ordering hack.
        builder = (
            builder.add(s2_mapped.GetGranuleDir.map(granule_id)("GetInnerDir"))
            .add(s2_mapped.CheckSolarZenith.map(granule_id)("CheckSolar"))
            .add(s2_mapped.FindFootprint.map(granule_id)(name="FindFootprint"))
            .add(s2_mapped.ApplyQualityMask.map(granule_id)(name="ApplyMask"))
            .add(s2_mapped.DeriveAngles.map(granule_id)(name="DeriveAngles"))
            .add(s2_lasrc.PrepareEspaInputNoFmask.map(granule_id)("PrepareEspa"))
            .add(s2_mapped.RunLaSRC.map(granule_id)("LaSRC"))
        )

    upload = s2_lasrc.UploadLaSRCDebug.map(granule_id)(
        "Upload", prefix=f"lasrc-port/{lasrc_version}"
    )
    return builder.add(upload)


def _build_landsat(
    granule_id: str,
    lasrc_version: LASRC_VERSION,
    working_dir: Path | None,
    local_granule: Path | None,
) -> PipelineBuilder:
    """Landsat just-LaSRC builder."""
    download: l_tasks.LocalGranule | l_tasks.DownloadGranule
    if local_granule:
        download = l_tasks.LocalGranule("LocalGranule", local_granule_dir=local_granule)
    else:
        download = l_tasks.DownloadGranule("DownloadGranule")

    builder = (
        PipelineBuilder()
        .add(
            l_tasks.EnvSource(
                "EnvConfig", granule_id=granule_id, working_dir=working_dir
            )
        )
        .add(download)
        .add(l_tasks.CheckSolarZenith("CheckSolar"))
    )

    if lasrc_version == "rust":
        builder = builder.add(l_lasrc.RunLaSRCRust("LaSRC"))
    else:
        builder = (
            builder.add(l_lasrc.ScanlineNoFmask("Scanline"))
            .add(l_tasks.ConvertToEspa("EspaConv"))
            .add(l_tasks.RunLaSRC("LaSRC"))
        )

    upload = l_lasrc.UploadLaSRCDebug("Upload", prefix=f"lasrc-port/{lasrc_version}")
    return builder.add(upload)
