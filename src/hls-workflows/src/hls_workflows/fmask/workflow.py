"""Construct the standalone ``fmask`` pipeline.

One unified ``construct_pipeline`` detects whether the granule is Sentinel-2 or
Landsat and assembles the minimal chain needed to reach Fmask, reusing the
existing per-mission tasks:

- Sentinel-2: ``Download -> GetInnerDir -> ApplyMask -> Fmask -> Upload``. The
  ESA quality mask is applied first because it changes the radiances Fmask
  reads in production.
- Landsat: ``Download -> Fmask -> Upload``.

Neither path runs the solar zenith check, so Fmask runs on granules that the
production pipelines would skip.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from hls_workflows.granules import LandsatGranule, Sentinel2Granule, resolve_granule
from hls_workflows.landsat_ac import tasks as l_tasks
from hls_workflows.metrics import MetricsCollector, MetricSink
from hls_workflows.pipeline import Pipeline, PipelineBuilder
from hls_workflows.sentinel import mapped_tasks as s2_mapped
from hls_workflows.sentinel import tasks as s2_tasks

from . import landsat as l_fmask
from . import sentinel as s2_fmask

logger = logging.getLogger(__name__)

UPLOAD_PREFIX = "fmask"


def construct_pipeline(
    granule_id: str,
    *,
    working_dir: Path | None = None,
    local_granule: Path | None = None,
    metric_sink: MetricSink | None = None,
    cleanup_working_dir: bool = True,
) -> Pipeline:
    """Construct the standalone Fmask pipeline for one granule.

    Parameters
    ----------
    granule_id
        Sentinel-2 or Landsat granule ID (auto-detected).
    working_dir
        Override local processing directory.
    local_granule
        Pre-downloaded granule path (a .zip for Sentinel-2, a directory for
        Landsat). If omitted, the granule is downloaded from S3.
    metric_sink
        Optional metrics sink.
    cleanup_working_dir
        Remove the working directory when the pipeline finishes.
    """
    match resolve_granule(granule_id):
        case Sentinel2Granule():
            builder = _build_sentinel(granule_id, working_dir, local_granule)
        case LandsatGranule():
            builder = _build_landsat(granule_id, working_dir, local_granule)
        case other:
            raise ValueError(
                f"Expected a Sentinel-2 or Landsat granule ID, got {type(other).__name__}"
            )

    return builder.build(
        metrics=MetricsCollector(
            pipeline_dims={"workflow": "fmask-workflow"},
            pipeline_props={"input_granule_id": granule_id},
            sink=metric_sink,
        ),
        cleanup_working_dir=cleanup_working_dir,
    )


def _build_sentinel(
    granule_id: str,
    working_dir: Path | None,
    local_granule: Path | None,
) -> PipelineBuilder:
    """Sentinel-2 ``fmask`` pipeline builder (single granule, no twin consolidation)."""
    # The Sentinel EnvSource reads GRANULE_LIST from the environment.
    os.environ["GRANULE_LIST"] = granule_id

    download: s2_mapped.LocalSentinelGranule | s2_mapped.DownloadSentinelGranule
    if local_granule:
        download = s2_mapped.LocalSentinelGranule.map(granule_id)(
            "LocalGranule", local_granule_zip=local_granule
        )
    else:
        download = s2_mapped.DownloadSentinelGranule.map(granule_id)("Download")

    return (
        PipelineBuilder()
        .add(s2_tasks.EnvSource("EnvConfig", working_dir=working_dir))
        .add(download)
        .add(s2_mapped.GetGranuleDir.map(granule_id)("GetInnerDir"))
        .add(s2_fmask.ApplyQualityMaskNoSolar.map(granule_id)(name="ApplyMask"))
        .add(s2_mapped.RunFmaskV5.map(granule_id)("Fmask"))
        .add(s2_fmask.UploadFmaskDebug.map(granule_id)("Upload", prefix=UPLOAD_PREFIX))
    )


def _build_landsat(
    granule_id: str,
    working_dir: Path | None,
    local_granule: Path | None,
) -> PipelineBuilder:
    """Landsat ``fmask`` pipeline builder."""
    download: l_tasks.LocalGranule | l_tasks.DownloadGranule
    if local_granule:
        download = l_tasks.LocalGranule("LocalGranule", local_granule_dir=local_granule)
    else:
        download = l_tasks.DownloadGranule("DownloadGranule")

    return (
        PipelineBuilder()
        .add(
            l_tasks.EnvSource(
                "EnvConfig", granule_id=granule_id, working_dir=working_dir
            )
        )
        .add(download)
        .add(l_tasks.RunFmaskV5("Fmask"))
        .add(l_fmask.UploadFmaskDebug("Upload", prefix=UPLOAD_PREFIX))
    )
