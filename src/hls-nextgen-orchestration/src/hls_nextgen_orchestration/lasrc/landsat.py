"""Landsat tasks specific to the standalone "just-LaSRC" pipeline.

Kept out of the production ``landsat_ac`` package so the main L30-AC pipeline
stays clean; this whole module is expected to be removed once the Rust LaSRC is
validated and wired into the main pipeline.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

from hls_nextgen_orchestration.base import Asset, AssetBundle, Task
from hls_nextgen_orchestration.common.lasrc_aux import resolve_lasrc_aux_paths
from hls_nextgen_orchestration.landsat_ac.assets import (
    CONFIG,
    GRANULE_DIR,
    LASRC_DONE,
    UPLOAD_COMPLETE,
    EnvConfig,
)
from hls_nextgen_orchestration.landsat_ac.tasks import ConvertScanline

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScanlineNoFmask(ConvertScanline):
    """ConvertScanline without the Fmask ordering dependency.

    ConvertScanline normally requires ``FMASK_BIN`` solely to force Fmask (which
    needs the original tiled TIFs) to run first. The standalone LaSRC pipeline
    runs no Fmask, so that ordering constraint is dropped.
    """

    requires = (GRANULE_DIR,)


@dataclass(frozen=True)
class RunLaSRCRust(Task):
    """Run the Rust LaSRC directly on the raw Landsat scene.

    Calls the Rust ``lasrc.pipeline.process_scene`` Python API (imported lazily
    so the linux-64-only package isn't required to build pipelines or run unit
    tests). Unlike the C path it consumes the downloaded granule directory and
    resolves its ancillary inputs from ``LASRC_AUX_DIR``; no ESPA conversion is
    required. Output is written in ESPA format for intercomparison with the C
    LaSRC.
    """

    instrument = True
    requires = (CONFIG, GRANULE_DIR)
    provides = (LASRC_DONE,)

    def run(self, inputs: AssetBundle) -> dict[Asset[bool], bool]:
        from lasrc.pipeline import AuxFilePaths, process_scene

        config: EnvConfig = inputs[CONFIG]
        granule_dir: Path = inputs[GRANULE_DIR]

        granule = config.landsat_granule
        aux_files = AuxFilePaths(
            **resolve_lasrc_aux_paths(
                is_sentinel=False, acquisition=granule.acquisition_date
            )
        )

        output_dir = granule_dir / "lasrc_rs_output"
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Run LaSRC (rust)")
        # platform "LC09" -> sensor "LANDSAT_9"
        process_scene(
            input_path=granule_dir,
            aux_files=aux_files,
            output_path=output_dir,
            sensor_name=f"LANDSAT_{granule.platform[-1]}",
            output_format="espa",
        )

        return {LASRC_DONE: True}


@dataclass(frozen=True)
class UploadLaSRCDebug(Task):
    """Recursively upload the granule dir to the debug bucket (just-LaSRC pipeline).

    Lightweight upload for the standalone LaSRC pipeline, which stops after
    LaSRC and has none of the downstream products the full ``UploadResults``
    requires. No-ops (with a warning) when ``DEBUG_BUCKET`` is unset.
    """

    requires = (CONFIG, LASRC_DONE)
    provides = (UPLOAD_COMPLETE,)

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]

        if not config.debug_bucket:
            logger.warning("DEBUG_BUCKET not set; skipping LaSRC debug upload")
            return {UPLOAD_COMPLETE: True}

        s3: S3Client = boto3.client("s3")
        timestamp = dt.datetime.now().strftime("%Y_%m_%d_%H_%M")
        base_key = f"{config.granule}_{timestamp}"
        logger.info(
            f"Uploading LaSRC debug files to s3://{config.debug_bucket}/{base_key}"
        )

        for f in config.granule_dir.rglob("*"):
            if f.is_file():
                rel_path = f.relative_to(config.granule_dir)
                key = f"{base_key}/{rel_path}"
                s3.upload_file(str(f), config.debug_bucket, key)

        return {UPLOAD_COMPLETE: True}
