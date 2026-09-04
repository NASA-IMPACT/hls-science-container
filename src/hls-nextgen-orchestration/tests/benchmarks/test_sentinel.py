from __future__ import annotations

import os
from pathlib import Path

import pytest

from hls_nextgen_orchestration.metrics import InMemorySink
from hls_nextgen_orchestration.sentinel.workflow import construct_pipeline

from .conftest import BenchmarkConfig, ResourceMetrics, granule_params


@pytest.mark.parametrize(
    "granule_id", granule_params(BenchmarkConfig.from_env().s2_granule_ids)
)
def test_s30(
    granule_id: str,
    s2_local_zips: dict[str, Path],
    work_dir: Path,
    resource_metrics: ResourceMetrics,
) -> None:
    if not granule_id:
        pytest.skip("BENCHMARK_S2_GRANULE_IDS not set")
    if granule_id not in s2_local_zips:
        pytest.skip("BENCHMARK_INPUT_BUCKET not set")
    # EnvSource reads GRANULE_LIST from env at pipeline.run() time
    os.environ["GRANULE_LIST"] = granule_id

    sink = InMemorySink()
    pipeline = construct_pipeline(
        granule_ids=[granule_id],
        working_dir=work_dir,
        local_granule_zips=[s2_local_zips[granule_id]],
        upload=False,
        metric_sink=sink,
    )

    # Runtime/memory/CPU come from the pipeline's own sampler (the InMemorySink).
    # Aggregate metrics here; per-task metrics are captured inside pipeline.run().
    with pipeline.metrics.collect_pipeline(
        pipeline_class="Pipeline", pipeline_name="sentinel-ac"
    ):
        pipeline.run()

    # "(v5)" suffix kept literal to preserve the benchmark chart series key.
    resource_metrics.add(f"{granule_id} (v5)", sink.records)
