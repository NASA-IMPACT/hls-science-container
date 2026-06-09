from __future__ import annotations

from pathlib import Path

import pytest

from hls_nextgen_orchestration.landsat_ac.workflow import construct_pipeline
from hls_nextgen_orchestration.metrics import InMemorySink

from .conftest import BenchmarkConfig, ResourceMetrics, granule_params


@pytest.mark.parametrize(
    "granule_id", granule_params(BenchmarkConfig.from_env().ls_granule_ids)
)
def test_l30_ac(
    benchmark: pytest.FixtureRequest,
    granule_id: str,
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
        fmask_version="v5",
        upload=False,
        metric_sink=sink,
    )

    def run() -> None:
        # Aggregate metrics; per-task metrics are captured inside pipeline.run().
        with pipeline.metrics.collect_pipeline(
            pipeline_class="Pipeline", pipeline_name="landsat-ac"
        ):
            pipeline.run()

    benchmark.pedantic(run, rounds=1, iterations=1)  # type: ignore[attr-defined]
    resource_metrics.add(granule_id, sink.records)
