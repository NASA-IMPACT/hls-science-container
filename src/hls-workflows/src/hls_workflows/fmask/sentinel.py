"""Sentinel-2 tasks specific to the standalone ``fmask`` pipeline.

Kept out of the production ``sentinel`` package so the main S30 pipeline stays
clean.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3

from hls_workflows.base import AssetBundle, MappedTask
from hls_workflows.common import S3Path
from hls_workflows.fmask.products import is_fmask_product
from hls_workflows.sentinel.assets import (
    CONFIG,
    UPLOAD_COMPLETE,
    EnvConfig,
    fmask_bin_asset,
    granule_dir_asset,
)
from hls_workflows.sentinel.mapped_tasks import ApplyQualityMask

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class ApplyQualityMaskNoSolar(ApplyQualityMask):
    """ApplyQualityMask without the solar zenith check dependency.

    The standalone Fmask pipeline runs Fmask regardless of the solar zenith
    angle, so it never runs ``CheckSolarZenith``.
    """

    requires_factory = lambda gid: (granule_dir_asset(gid),)


@dataclass(frozen=True, kw_only=True)
class UploadFmaskDebug(MappedTask):
    """Upload the Fmask cloud mask to the debug bucket.

    Uploads only the ``*_UPL.tif`` mask, flattened into a single
    ``<prefix>/<granule_id>/`` directory. Depends on the Fmask ENVI binary
    purely to order after Fmask.

    No-ops (with a warning) when ``DEBUG_BUCKET`` is unset.
    """

    prefix: str

    requires_factory = lambda gid: (CONFIG, fmask_bin_asset(gid))
    provides = (UPLOAD_COMPLETE,)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        config: EnvConfig = bundle[CONFIG]

        if not config.debug_bucket:
            logger.warning("DEBUG_BUCKET not set; skipping Fmask debug upload")
            return {UPLOAD_COMPLETE: True}

        s3: S3Client = boto3.client("s3")
        base = S3Path(config.debug_bucket, f"{self.prefix}/{self.granule_id}")
        logger.info(f"Uploading Fmask debug files to {base}")

        for f in (config.working_dir / self.granule_id).rglob("*"):
            if f.is_file() and is_fmask_product(f.name):
                dest = base / f.name
                s3.upload_file(str(f), dest.bucket, dest.key)

        return {UPLOAD_COMPLETE: True}
