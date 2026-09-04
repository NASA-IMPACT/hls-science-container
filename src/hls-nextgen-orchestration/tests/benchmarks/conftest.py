from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import pytest

from hls_nextgen_orchestration.metrics import MetricRecord

if TYPE_CHECKING:
    from _pytest.mark.structures import ParameterSet

logger = logging.getLogger(__name__)


def _available_cpus() -> int:
    """CPUs usable by this process (respects cgroup/affinity limits on Linux)."""
    sched_getaffinity = getattr(os, "sched_getaffinity", None)  # Linux only
    if sched_getaffinity is not None:
        return len(sched_getaffinity(0))
    return os.cpu_count() or 1


def pytest_configure(config: pytest.Config) -> None:
    """Stream INFO logs live during benchmark runs (e.g. the S3 sync progress).

    Scoped to the benchmarks via this conftest, so the main test suite is
    unaffected and rootdir stays the repo root (preserving benchmark fullnames /
    chart series keys). Equivalent to passing ``--log-cli-level=INFO``, but only
    when the caller hasn't already set a level.
    """
    if config.option.log_cli_level is None:
        config.option.log_cli_level = "INFO"


@dataclass(frozen=True)
class BenchmarkConfig:
    """Benchmark configuration sourced from ``BENCHMARK_*`` environment variables.

    Built once via `from_env` — as the ``config`` session fixture for the
    runtime fixtures, and at module scope in the test files where
    parametrization needs the granule IDs at collection time.
    """

    aux_bucket: str = ""
    aux_prefix: str = ""
    input_bucket: str = ""
    input_prefix: str = ""
    s2_granule_ids: list[str] = field(default_factory=list)
    ls_granule_ids: list[str] = field(default_factory=list)
    num_threads: int = 2  # clamped to available CPUs at runtime

    @classmethod
    def from_env(cls) -> BenchmarkConfig:
        return cls(
            aux_bucket=os.environ.get("BENCHMARK_AUX_BUCKET", ""),
            aux_prefix=os.environ.get("BENCHMARK_AUX_PREFIX", ""),
            input_bucket=os.environ.get("BENCHMARK_INPUT_BUCKET", ""),
            input_prefix=os.environ.get("BENCHMARK_INPUT_PREFIX", ""),
            s2_granule_ids=_split_ids("BENCHMARK_S2_GRANULE_IDS"),
            ls_granule_ids=_split_ids("BENCHMARK_LS_GRANULE_IDS"),
            num_threads=int(os.environ.get("BENCHMARK_NUM_THREADS", "2")),
        )


def _split_ids(var: str) -> list[str]:
    return [g for g in os.environ.get(var, "").split(",") if g]


def granule_params(granule_ids: list[str]) -> list[ParameterSet]:
    """Build parametrize params keyed by granule ID (the chart series key).

    Falls back to a single placeholder param when no IDs are configured, so the
    test is collected and visibly skips rather than silently disappearing.
    """
    if not granule_ids:
        return [pytest.param("", id="env-not-set")]
    return [pytest.param(gid, id=gid) for gid in granule_ids]


def _s3_sync(bucket: str, prefix: str, local_dir: Path) -> None:
    # Bucket is a secret (masked in CI logs); log only the prefix/key, which
    # are non-secret and are what you actually need to debug the layout.
    logger.info("Syncing %s -> %s", prefix, local_dir)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) :].lstrip("/")
            if not rel:
                continue
            dest = local_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            logger.info("%s -> %s", key, dest)
            s3.download_file(bucket, key, str(dest))
            count += 1

    if count == 0:
        logger.warning("No objects found under prefix %s — check the prefix", prefix)
    else:
        logger.info("Synced %d object(s) from prefix %s", count, prefix)


@pytest.fixture(scope="session")
def config() -> BenchmarkConfig:
    return BenchmarkConfig.from_env()


@pytest.fixture
def work_dir() -> Iterator[Path]:
    """Per-test working dir, removed right after (unlike pytest's tmp_path)."""
    d = Path(tempfile.mkdtemp(prefix="benchmark-"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def lasrc_aux_dir(
    config: BenchmarkConfig, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Ensure LASRC_AUX_DIR is set before any benchmark runs.

    If LASRC_AUX_DIR is already set (e.g. scripts/shell mounts data/lasrc_aux/
    into the container), the existing value is used as-is.  Otherwise the aux
    data is synced from S3 using BENCHMARK_AUX_BUCKET + BENCHMARK_AUX_PREFIX
    and the env var is pointed at the downloaded copy.
    """
    if os.environ.get("LASRC_AUX_DIR"):
        return

    if not config.aux_bucket or not config.aux_prefix:
        pytest.skip(
            "LASRC_AUX_DIR not set and BENCHMARK_AUX_BUCKET/AUX_PREFIX not configured"
        )

    aux_dir = tmp_path_factory.mktemp("lasrc_aux")
    os.environ["LASRC_AUX_DIR"] = str(aux_dir)

    logger.info(f"Fetching auxiliary data from {config.aux_prefix}...")
    _s3_sync(config.aux_bucket, config.aux_prefix, aux_dir)


@pytest.fixture(scope="session", autouse=True)
def benchmark_env(config: BenchmarkConfig) -> None:
    """Set env vars required by workflow EnvSource nodes.

    Pipelines read OUTPUT_BUCKET, ACCODE, etc. from os.environ at runtime
    even when using local input data (upload=False). Set non-sensitive
    placeholders so the workflows initialize cleanly.

    GRANULE_LIST is intentionally not set here — each Sentinel benchmark
    test sets it to its own granule_id before calling pipeline.run().
    """
    # Pin threads (clamped to available CPUs) for consistent timings; inherited
    # by child subprocesses (LaSRC, Fmask).
    cpus = _available_cpus()
    threads = max(1, min(config.num_threads, cpus))
    os.environ["OMP_NUM_THREADS"] = str(threads)
    logger.info("Pinned OMP_NUM_THREADS=%d (%d CPUs available)", threads, cpus)

    defaults: dict[str, str] = {
        "ACCODE": "LaSRC v3.5.1.0",
        "INPUT_BUCKET": config.input_bucket,
        "OUTPUT_BUCKET": "benchmark-output-unused",
        "GIBS_OUTPUT_BUCKET": "benchmark-gibs-unused",
        "PREFIX": "L30",
    }
    for key, value in defaults.items():
        if value and key not in os.environ:
            os.environ[key] = value


@pytest.fixture(scope="session")
def s2_local_zips(
    config: BenchmarkConfig, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, Path]:
    """Download all Sentinel-2 benchmark granule zips from S3 once per session.

    Returns a mapping of granule_id -> local zip path.
    Expected S3 key: ``{BENCHMARK_INPUT_PREFIX}/{granule_id}.zip``.
    """
    if not config.s2_granule_ids or not config.input_bucket or not config.input_prefix:
        return {}

    session_dir = tmp_path_factory.mktemp("s2_data")
    s3 = boto3.client("s3")
    result: dict[str, Path] = {}
    for granule_id in config.s2_granule_ids:
        zip_path = session_dir / f"{granule_id}.zip"
        s3.download_file(
            config.input_bucket,
            f"{config.input_prefix}/{granule_id}.zip",
            str(zip_path),
        )
        result[granule_id] = zip_path
    return result


@pytest.fixture(scope="session")
def ls_local_dirs(
    config: BenchmarkConfig, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, Path]:
    """Download all Landsat benchmark granule directories from S3 once per session.

    Returns a mapping of granule_id -> local directory path.
    Expected S3 path per granule:
    ``s3://{BENCHMARK_INPUT_BUCKET}/{BENCHMARK_INPUT_PREFIX}/{granule_id}/``.
    """
    if not config.ls_granule_ids or not config.input_bucket or not config.input_prefix:
        return {}

    session_dir = tmp_path_factory.mktemp("ls_data")
    result: dict[str, Path] = {}
    for granule_id in config.ls_granule_ids:
        granule_dir = session_dir / granule_id
        granule_dir.mkdir()
        _s3_sync(
            config.input_bucket, f"{config.input_prefix}/{granule_id}/", granule_dir
        )
        result[granule_id] = granule_dir
    return result


@dataclass
class ResourceMetrics:
    """Accumulates per-granule :class:`MetricRecord`s from the pipeline's own
    sampler and writes a single github-action-benchmark ``customSmallerIsBetter``
    JSON covering runtime, peak memory, and CPU per task/pipeline — so one chart
    group (and one publish step) tracks all three metrics together.
    """

    # (series_label, records) — label is the bracket key, e.g. "<granule> (v5)".
    _items: list[tuple[str, list[MetricRecord]]] = field(default_factory=list)

    def add(self, label: str, records: list[MetricRecord]) -> None:
        self._items.append((label, records))

    def write(self) -> None:
        if not self._items:
            return

        entries: list[dict[str, object]] = []
        for label, records in self._items:
            for rec in records:
                for metric, unit, value in (
                    ("runtime_seconds", "s", rec.runtime_seconds),
                    ("peak_memory_mb", "MB", rec.peak_memory_mb),
                    ("avg_cpu_percent", "%", rec.avg_cpu_percent),
                ):
                    entries.append(
                        {
                            "name": f"{rec.task_name} {metric} [{label}]",
                            "unit": unit,
                            "value": value,
                        }
                    )

        out_dir = Path(os.environ.get("BENCHMARK_OUTPUT_DIR", "benchmark-output"))
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / "resources.json"
        out.write_text(json.dumps(entries, indent=2))
        logger.info("Wrote %d benchmark metric(s) to %s", len(entries), out)


@pytest.fixture(scope="session")
def resource_metrics() -> Iterator[ResourceMetrics]:
    collector = ResourceMetrics()
    yield collector
    collector.write()
