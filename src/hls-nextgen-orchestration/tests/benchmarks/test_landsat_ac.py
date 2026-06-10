from __future__ import annotations

from pathlib import Path

import pytest

from hls_nextgen_orchestration.constants import FMASK_VERSION
from hls_nextgen_orchestration.landsat_ac.workflow import construct_pipeline
from hls_nextgen_orchestration.metrics import InMemorySink

from .conftest import BenchmarkConfig, ResourceMetrics, fmask_versions, granule_params


@pytest.mark.parametrize("fmask_version", fmask_versions())
@pytest.mark.parametrize(
    "granule_id", granule_params(BenchmarkConfig.from_env().ls_granule_ids)
)
def test_l30_ac(
    granule_id: str,
    fmask_version: FMASK_VERSION,
    ls_local_dirs: dict[str, Path],
    tmp_path: Path,
    resource_metrics: ResourceMetrics,
) -> None:
    if not granule_id:
        pytest.skip("BENCHMARK_LS_GRANULE_IDS not set")
    if granule_id not in ls_local_dirs:
        pytest.skip("BENCHMARK_INPUT_BUCKET not set")

    sink = InMemorySink()
    pipeline = construct_pipeline(
        granule_id=granule_id,
        working_dir=tmp_path,
        local_granule_dir=ls_local_dirs[granule_id],
        fmask_version=fmask_version,
        upload=False,
        metric_sink=sink,
    )

    # Runtime/memory/CPU come from the pipeline's own sampler (the InMemorySink).
    # Aggregate metrics here; per-task metrics are captured inside pipeline.run().
    with pipeline.metrics.collect_pipeline(
        pipeline_class="Pipeline", pipeline_name="landsat-ac"
    ):
        pipeline.run()

    resource_metrics.add(f"{granule_id} ({fmask_version})", sink.records)
