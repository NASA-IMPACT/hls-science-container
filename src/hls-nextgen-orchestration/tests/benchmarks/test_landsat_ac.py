from __future__ import annotations

from pathlib import Path

import pytest

from hls_nextgen_orchestration.landsat_ac.workflow import construct_pipeline

from .conftest import BenchmarkConfig, granule_params


@pytest.mark.parametrize(
    "granule_id", granule_params(BenchmarkConfig.from_env().ls_granule_ids)
)
def test_l30_ac(
    benchmark: pytest.FixtureRequest,
    granule_id: str,
    ls_local_dirs: dict[str, Path],
    tmp_path: Path,
) -> None:
    if not granule_id:
        pytest.skip("BENCHMARK_LS_GRANULE_IDS not set")
    if granule_id not in ls_local_dirs:
        pytest.skip("BENCHMARK_INPUT_BUCKET not set")
    pipeline = construct_pipeline(
        granule_id=granule_id,
        working_dir=tmp_path,
        local_granule_dir=ls_local_dirs[granule_id],
        fmask_version="v5",
        upload=False,
    )
    benchmark.pedantic(pipeline.run, rounds=1, iterations=1)  # type: ignore[attr-defined]
