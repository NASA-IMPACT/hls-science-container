from __future__ import annotations

import logging
import sys

from hls_nextgen_orchestration.base import Pipeline, PipelineBuilder

from .assets import (
    ANGLE_HDF,
    CMR_XML,
    COGS_CREATED,
    CONFIG,
    GIBS_DIR,
    GRIDDED_HDF,
    MANIFEST_FILE,
    NBAR_ANGLE,
    NBAR_INPUT,
    OUTPUT_BASE_NAME,
    OUTPUT_HDF,
    SCENE_TIME,
    STAC_JSON,
    THUMBNAIL_FILE,
    UPLOAD_COMPLETE,
    VI_DIR,
)
from .tasks import (
    ConvertToCogs,
    CreateManifest,
    CreateMetadata,
    CreateThumbnail,
    EnvSource,
    ProcessGibs,
    ProcessPathRows,
    ProcessVi,
    RunNbar,
    UploadAll,
)


def construct_pipeline() -> Pipeline:
    return (
        PipelineBuilder()
        .add(EnvSource("EnvConfig", provides=(CONFIG,)))
        .add(
            ProcessPathRows(
                "ProcessPathRows",
                requires=(CONFIG,),
                provides=(NBAR_INPUT, NBAR_ANGLE, SCENE_TIME, OUTPUT_BASE_NAME),
            )
        )
        .add(
            RunNbar(
                "RunNbar",
                requires=(
                    CONFIG,
                    NBAR_INPUT,
                    NBAR_ANGLE,
                    SCENE_TIME,
                    OUTPUT_BASE_NAME,
                ),
                provides=(OUTPUT_HDF, ANGLE_HDF, GRIDDED_HDF),
            )
        )
        .add(
            ConvertToCogs(
                "ConvertToCogs",
                requires=(CONFIG, OUTPUT_HDF, ANGLE_HDF),
                provides=(COGS_CREATED,),
            )
        )
        .add(
            CreateThumbnail(
                "CreateThumbnail",
                requires=(CONFIG, OUTPUT_BASE_NAME),
                provides=(THUMBNAIL_FILE,),
            )
        )
        .add(
            CreateMetadata(
                "CreateMetadata",
                requires=(CONFIG, OUTPUT_HDF, OUTPUT_BASE_NAME),
                provides=(CMR_XML, STAC_JSON),
            )
        )
        .add(
            CreateManifest(
                "CreateManifest",
                requires=(
                    CONFIG,
                    OUTPUT_BASE_NAME,
                    COGS_CREATED,
                    THUMBNAIL_FILE,
                    CMR_XML,
                ),
                provides=(MANIFEST_FILE,),
            )
        )
        .add(
            ProcessGibs(
                "ProcessGibs",
                requires=(CONFIG, OUTPUT_BASE_NAME),
                provides=(GIBS_DIR,),
            )
        )
        .add(
            ProcessVi(
                "ProcessVi",
                requires=(CONFIG, OUTPUT_BASE_NAME),
                provides=(VI_DIR,),
            )
        )
        .add(
            UploadAll(
                "UploadAll",
                requires=(
                    CONFIG,
                    OUTPUT_BASE_NAME,
                    GIBS_DIR,
                    VI_DIR,
                    GRIDDED_HDF,
                    MANIFEST_FILE,
                ),
                provides=(UPLOAD_COMPLETE,),
            )
        )
        .build()
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        pipeline = construct_pipeline()
        print(pipeline)
        context = pipeline.run()
        sys.exit(context.exit_code)
    except Exception as e:
        logging.error(f"Pipeline failed initialization: {e}")
        sys.exit(1)
