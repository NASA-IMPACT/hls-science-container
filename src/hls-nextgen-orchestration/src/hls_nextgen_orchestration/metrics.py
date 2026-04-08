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

from hls_nextgen_orchestration.base import MappedTask, NodeBase

if TYPE_CHECKING:
    from mypy_boto3_logs import CloudWatchLogsClient

logger = logging.getLogger(__name__)

_EXPERIMENT_PREFIX = "HLS_EXPERIMENT_"
_NAMESPACE = "HLS/Tasks"


@dataclass
class _Sample:
    peak_memory_mb: float = 0.0
    max_cpu_percent: float = 0.0


@dataclass(eq=False)
class _PollingThread(threading.Thread):
    """Polls the current process tree for peak memory and max CPU."""

    interval: float = 1.0
    sample: _Sample = field(default_factory=_Sample, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _proc: psutil.Process = field(default_factory=psutil.Process, init=False)

    def __post_init__(self) -> None:
        super().__init__(daemon=True)

    def run(self) -> None:
        # Prime cpu_percent — first call always returns 0.0
        self._proc.cpu_percent(interval=None)
        for child in self._proc.children(recursive=True):
            try:
                child.cpu_percent(interval=None)
            except psutil.NoSuchProcess:
                pass

        while not self._stop.wait(self.interval):
            self._poll()

    def _poll(self) -> None:
        try:
            procs = [self._proc] + self._proc.children(recursive=True)
            rss = 0
            cpu = 0.0
            for p in procs:
                try:
                    rss += p.memory_info().rss
                    cpu += p.cpu_percent(interval=None)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            mb = rss / (1024 * 1024)
            self.sample.peak_memory_mb = max(self.sample.peak_memory_mb, mb)
            self.sample.max_cpu_percent = max(self.sample.max_cpu_percent, cpu)
        except Exception:
            logger.debug("Metrics poll error", exc_info=True)

    def stop(self) -> _Sample:
        self._stop.set()
        self.join()
        return self.sample


@dataclass
class _MetricsContext:
    """Context manager that wraps a single task execution."""

    collector: MetricsCollector
    node: NodeBase
    _poller: _PollingThread | None = field(default=None, init=False)
    _start: float = field(default=0.0, init=False)

    def __enter__(self) -> _MetricsContext:
        self._start = time.perf_counter()
        self._poller = _PollingThread()
        self._poller.start()
        return self

    def __exit__(self, *_: object) -> None:
        runtime = time.perf_counter() - self._start
        sample = self._poller.stop() if self._poller else _Sample()
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

    _log_group: str | None = field(
        default_factory=lambda: os.environ.get("METRIC_LOG_GROUP_NAME"), init=False
    )
    _job_id: str = field(
        default_factory=lambda: os.environ.get("AWS_BATCH_JOB_ID", "local_job"),
        init=False,
    )
    _experiment_dims: dict[str, str] = field(
        default_factory=lambda: {
            key.removeprefix(_EXPERIMENT_PREFIX).lower(): val
            for key, val in os.environ.items()
            if key.startswith(_EXPERIMENT_PREFIX)
        },
        init=False,
    )
    _logs: CloudWatchLogsClient = field(
        default_factory=lambda: boto3.client("logs"), init=False
    )
    enabled: bool = field(init=False)

    def __post_init__(self) -> None:
        self.enabled = bool(self._log_group)

    def collect(self, node: NodeBase) -> AbstractContextManager[_MetricsContext | None]:
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
        if isinstance(node, MappedTask) and "granule_id" in type(node).__dict__:
            fixed["granule_id"] = type(node).__dict__["granule_id"]

        dims = {**fixed, **self._experiment_dims}

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
                            {"Name": "max_cpu_percent", "Unit": "Percent"},
                        ],
                    }
                ],
            },
            **dims,
            "runtime_seconds": round(runtime, 3),
            "peak_memory_mb": round(sample.peak_memory_mb, 1),
            "max_cpu_percent": round(sample.max_cpu_percent, 1),
        }

        assert self._log_group is not None
        try:
            self._logs.put_log_events(
                logGroupName=self._log_group,
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
                f"max_cpu={sample.max_cpu_percent:.0f}%"
            )
        except Exception:
            logger.warning("Failed to emit task metrics", exc_info=True)
