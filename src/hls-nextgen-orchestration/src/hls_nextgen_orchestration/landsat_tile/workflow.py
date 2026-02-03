from __future__ import annotations

import logging
import sys
from pathlib import Path

from hls_nextgen_orchestration.base import Pipeline, PipelineBuilder

from .assets import (
    CONFIG,
    GIBS_DIR,
    GRIDDED_HDF,
    MANIFEST_FILE,
    OUTPUT_BASE_NAME,
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


def construct_pipeline(
    working_dir: Path | None = None,
) -> Pipeline:
    return (
        PipelineBuilder()
        .add(EnvSource("EnvConfig", working_dir=working_dir))
        .add(ProcessPathRows("ProcessPathRows"))
        .add(RunNbar("RunNbar"))
        .add(ConvertToCogs("ConvertToCogs"))
        .add(CreateThumbnail("CreateThumbnail"))
        .add(CreateMetadata("CreateMetadata"))
        .add(CreateManifest("CreateManifest"))
        .add(ProcessGibs("ProcessGibs"))
        .add(ProcessVi("ProcessVi"))
        .add(
            UploadAll(
                "UploadAll"
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
