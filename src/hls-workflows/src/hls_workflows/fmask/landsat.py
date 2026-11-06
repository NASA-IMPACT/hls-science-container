"""Landsat tasks specific to the standalone "just-Fmask" pipeline.

Kept out of the production ``landsat_ac`` package so the main L30-AC pipeline
stays clean.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3

from hls_workflows.base import AssetBundle, Task
from hls_workflows.common import S3Path
from hls_workflows.fmask.products import is_fmask_product
from hls_workflows.landsat_ac.assets import (
    CONFIG,
    FMASK_BIN,
    UPLOAD_COMPLETE,
    EnvConfig,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadFmaskDebug(Task):
    """Upload the Fmask cloud mask to the debug bucket.

    Uploads only the ``*_UPL.tif`` mask, flattened into a single
    ``<prefix>/<granule_id>/`` directory. Depends on the Fmask ENVI binary
    purely to order after Fmask.

    No-ops (with a warning) when ``DEBUG_BUCKET`` is unset.
    """

    prefix: str

    requires = (CONFIG, FMASK_BIN)
    provides = (UPLOAD_COMPLETE,)

    def run(self, inputs: AssetBundle) -> AssetBundle:
        config: EnvConfig = inputs[CONFIG]

        if not config.debug_bucket:
            logger.warning("DEBUG_BUCKET not set; skipping Fmask debug upload")
            return {UPLOAD_COMPLETE: True}

        s3: S3Client = boto3.client("s3")
        base = S3Path(config.debug_bucket, f"{self.prefix}/{config.granule}")
        logger.info(f"Uploading Fmask debug files to {base}")

        for f in config.granule_dir.rglob("*"):
            if f.is_file() and is_fmask_product(f.name):
                dest = base / f.name
                s3.upload_file(str(f), dest.bucket, dest.key)

        return {UPLOAD_COMPLETE: True}
