"""Sentinel-2 tasks specific to the standalone "just-LaSRC" pipeline.

Kept out of the production ``sentinel`` package so the main S30 pipeline stays
clean; this whole module is expected to be removed once the Rust LaSRC is
validated and wired into the main pipeline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3

from hls_nextgen_orchestration.base import AssetBundle, MappedTask, TaskFailure
from hls_nextgen_orchestration.common import S3Path
from hls_nextgen_orchestration.common.lasrc_aux import resolve_lasrc_aux_paths
from hls_nextgen_orchestration.granules import Sentinel2Granule
from hls_nextgen_orchestration.lasrc.products import is_sr_product
from hls_nextgen_orchestration.sentinel.assets import (
    CONFIG,
    UPLOAD_COMPLETE,
    EnvConfig,
    angle_hdf_asset,
    lasrc_aerosol_qa_asset,
    safe_dir_asset,
)
from hls_nextgen_orchestration.sentinel.mapped_tasks import PrepareEspaInput

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class PrepareEspaInputNoFmask(PrepareEspaInput):
    """PrepareEspaInput without the Fmask ordering dependency.

    PrepareEspaInput normally requires ``fmask_bin_asset`` solely to force Fmask
    to run before the SAFE is repackaged. The standalone LaSRC pipeline runs no
    Fmask, so that ordering constraint is dropped (the angle dependency, which
    LaSRC genuinely needs, is kept).
    """

    requires_factory = lambda gid: (CONFIG, safe_dir_asset(gid), angle_hdf_asset(gid))


@dataclass(frozen=True, kw_only=True)
class RunLaSRCRust(MappedTask):
    """Runs the Rust LaSRC for Sentinel directly on the SAFE scene.

    Output is written in ESPA format for intercomparison with the C LaSRC.
    """

    instrument = True
    requires_factory = lambda gid: (CONFIG, safe_dir_asset(gid))
    provides_factory = lambda gid: (lasrc_aerosol_qa_asset(gid),)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        # Sentinel has its own entry point (lasrc.pipeline.process_scene reads a
        # Landsat scene); it takes the aux paths as individual kwargs rather than
        # an AuxFilePaths bundle.
        from lasrc.pipeline_sentinel import process_sentinel_scene

        config: EnvConfig = bundle[CONFIG]
        safe_dir = bundle[safe_dir_asset(self.granule_id)]

        granule = Sentinel2Granule.from_str(self.granule_id)
        aux = resolve_lasrc_aux_paths(
            is_sentinel=True, acquisition=granule.acquisition_time
        )

        # Write straight into the granule working dir (un-nested); the writer
        # names the outputs <granule_id>_sr_*.img.
        output_dir = config.working_dir / self.granule_id
        output_dir.mkdir(parents=True, exist_ok=True)

        # mission "S2B" -> sensor "SENTINEL_2B"
        process_sentinel_scene(
            safe_dir=safe_dir,
            angle_hdf=aux["angle_hdf"],
            intref_hdf=aux["intref_hdf"],
            transm_hdf=aux["transm_hdf"],
            sphera_hdf=aux["sphera_hdf"],
            viirs_aux_path=aux["wv_oz_hdf"],
            dem_path=aux["dem_hdf"],
            ratio_path=aux["ratio_hdf"],
            output_path=output_dir,
            sensor_name=f"SENTINEL_2{granule.mission[-1]}",
            output_format="espa",
        )

        aerosol_qa = next(iter(output_dir.rglob("*aerosol_qa*.img")), None)
        if aerosol_qa is None:
            raise TaskFailure(
                "Cannot find the Rust LaSRC aerosol QA output "
                f"(expected '*aerosol_qa*.img' under {output_dir})"
            )

        return {lasrc_aerosol_qa_asset(self.granule_id): aerosol_qa}


@dataclass(frozen=True, kw_only=True)
class UploadLaSRCDebug(MappedTask):
    """Upload the LaSRC surface-reflectance products to the debug bucket.

    Lightweight upload for the standalone LaSRC pipeline, which stops after LaSRC and
    has none of the downstream products the full ``UploadAll`` requires.

    Uploads only the ``*_sr_band*`` / ``*_sr_aerosol*`` products, skipping inputs and
    ESPA intermediates. Depends on the LaSRC aerosol QA output purely to order after
    LaSRC.

    No-ops (with a warning) when ``DEBUG_BUCKET`` is unset.
    """

    prefix: str

    requires_factory = lambda gid: (CONFIG, lasrc_aerosol_qa_asset(gid))
    provides = (UPLOAD_COMPLETE,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]

        if not config.debug_bucket:
            logger.warning("DEBUG_BUCKET not set; skipping LaSRC debug upload")
            return {UPLOAD_COMPLETE: True}

        s3: S3Client = boto3.client("s3")
        base = S3Path(config.debug_bucket, f"{self.prefix}/{self.granule_id}")
        logger.info(f"Uploading LaSRC debug files to {base}")

        for f in config.working_dir.rglob("*"):
            if f.is_file() and is_sr_product(f.name):
                dest = base / str(f.relative_to(config.working_dir))
                s3.upload_file(str(f), dest.bucket, dest.key)

        return {UPLOAD_COMPLETE: True}
