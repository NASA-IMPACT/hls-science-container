from __future__ import annotations

import json
import logging
import os
import threading
import time
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import boto3
import psutil

from hls_nextgen_orchestration.base import NodeBase, TaskFailure

if TYPE_CHECKING:
    from mypy_boto3_logs import CloudWatchLogsClient

logger = logging.getLogger(__name__)

_EXPERIMENT_PREFIX = "HLS_EXPERIMENT_"
_NAMESPACE = "HLS/Tasks"


@dataclass
class _Sample:
    peak_memory_mb: float = 0.0
    avg_cpu_percent: float = 0.0
    exit_code: int = 0


@dataclass(eq=False)
class _PollingThread(threading.Thread):
    """Polls the current process tree for peak memory; measures avg CPU via cpu_times().

    CPU is measured by diffing proc.cpu_times() at start and stop rather than
    polling cpu_percent(). On Linux, cpu_times() includes children_user and
    children_system — the transitively accumulated CPU of all waited-for
    children (grandchildren roll up through the shell that reaps them). This
    naturally captures subprocess CPU without needing to track child PIDs.
    """

    interval: float = 1.0
    sample: _Sample = field(default_factory=_Sample, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _proc: psutil.Process = field(default_factory=psutil.Process, init=False)
    _cpu_start: float = field(default=0.0, init=False)
    _wall_start: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        super().__init__(daemon=True)

    def _cpu_secs(self) -> float:
        t = self._proc.cpu_times()
        return (
            t.user
            + t.system
            + getattr(t, "children_user", 0.0)
            + getattr(t, "children_system", 0.0)
        )

    def run(self) -> None:
        self._wall_start = time.monotonic()
        self._cpu_start = self._cpu_secs()
        while not self._stop.wait(self.interval):
            self._poll()

    def _poll(self) -> None:
        try:
            procs = [self._proc] + self._proc.children(recursive=True)
            rss = 0
            for p in procs:
                try:
                    with p.oneshot():
                        rss += p.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            self.sample.peak_memory_mb = max(
                self.sample.peak_memory_mb, rss / (1024 * 1024)
            )
        except Exception:
            logger.debug("Metrics poll error", exc_info=True)

    def stop(self) -> _Sample:
        self._stop.set()
        self.join()
        wall = time.monotonic() - self._wall_start
        if wall >= self.interval:
            self.sample.avg_cpu_percent = (
                (self._cpu_secs() - self._cpu_start) / wall * 100
            )
        return self.sample


@dataclass
class _MetricsContext:
    """Context manager that wraps a single task execution."""

    collector: MetricsCollector
    node: NodeBase
    _poller: _PollingThread | None = field(default=None, init=False)
    _start: float = field(default=0.0, init=False)

    def __enter__(self) -> None:
        self._start = time.perf_counter()
        self._poller = _PollingThread()
        self._poller.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        runtime = time.perf_counter() - self._start
        sample = self._poller.stop() if self._poller else _Sample()
        if exc_val is None:
            sample.exit_code = 0
        elif isinstance(exc_val, TaskFailure):
            sample.exit_code = exc_val.exit_code
        else:
            sample.exit_code = 1
        self.collector._emit(self.node, runtime, sample)


@dataclass
class MetricsCollector:
    """
    Collects task metrics and emits them to CloudWatch Logs in EMF format.

    Enabled when ``METRIC_LOG_GROUP_NAME`` is set in the environment.
    Experiment dimensions are sourced from any envvar prefixed with
    ``HLS_EXPERIMENT_`` (e.g. ``HLS_EXPERIMENT_FMASK_VERSION=v5`` adds
    dimension ``fmask_version=v5``).
    """

    log_group: str | None = field(
        default_factory=lambda: os.environ.get("METRIC_LOG_GROUP_NAME")
    )
    experiment_dims: dict[str, str] = field(
        default_factory=lambda: {
            key.removeprefix(_EXPERIMENT_PREFIX).lower(): val
            for key, val in os.environ.items()
            if key.startswith(_EXPERIMENT_PREFIX)
        },
    )
    pipeline_dims: dict[str, str] = field(default_factory=dict)
    client: CloudWatchLogsClient = field(default_factory=lambda: boto3.client("logs"))
    enabled: bool = field(init=False)
    _job_id: str = field(
        default_factory=lambda: os.environ.get("AWS_BATCH_JOB_ID", "local_job"),
        init=False,
    )

    def __post_init__(self) -> None:
        self.enabled = bool(self.log_group)
        if self.log_group:
            try:
                self.client.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=self._job_id,
                )
            except self.client.exceptions.ResourceAlreadyExistsException:
                pass

    def collect(self, node: NodeBase) -> AbstractContextManager[None]:
        """Return a context manager that measures a task's execution.

        Returns a no-op context manager if metrics are disabled or the node
        has not opted in via ``instrument = True``.
        """
        if not self.enabled or not node.instrument:
            return nullcontext()
        return _MetricsContext(self, node)

    def _emit(self, node: NodeBase, runtime: float, sample: _Sample) -> None:
        if not self.enabled:
            return

        fixed: dict[str, str] = {
            "task_class": type(node).__name__,
            "task_name": node.name,
            "job_id": self._job_id,
        }
        if granule_id := getattr(type(node), "granule_id", None):
            fixed["input_granule_id"] = granule_id

        dims = {**fixed, **self.pipeline_dims, **self.experiment_dims}

        record: dict[str, Any] = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": _NAMESPACE,
                        "Dimensions": [list(dims.keys())],
                        "Metrics": [
                            {"Name": "runtime_seconds", "Unit": "Seconds"},
                            {"Name": "peak_memory_mb", "Unit": "Megabytes"},
                            {"Name": "avg_cpu_percent", "Unit": "Percent"},
                            {"Name": "exit_code", "Unit": "None"},
                        ],
                    }
                ],
            },
            **dims,
            "runtime_seconds": round(runtime, 3),
            "peak_memory_mb": round(sample.peak_memory_mb, 1),
            "avg_cpu_percent": round(sample.avg_cpu_percent, 1),
            "exit_code": sample.exit_code,
        }

        assert self.log_group is not None
        try:
            self.client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self._job_id,
                logEvents=[
                    {
                        "timestamp": int(time.time() * 1000),
                        "message": json.dumps(record),
                    }
                ],
            )
            logger.debug(
                f"Emitted metrics for {node.name}: "
                f"runtime={runtime:.1f}s "
                f"peak_mem={sample.peak_memory_mb:.0f}MB "
                f"avg_cpu={sample.avg_cpu_percent:.0f}% "
                f"exit_code={sample.exit_code}"
            )
        except Exception:
            logger.warning("Failed to emit task metrics", exc_info=True)
