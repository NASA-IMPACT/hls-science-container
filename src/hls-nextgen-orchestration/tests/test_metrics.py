from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import nullcontext
from dataclasses import make_dataclass
from typing import Any

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_logs import CloudWatchLogsClient

from hls_nextgen_orchestration.base import (
    Asset,
    Assets,
    DataSource,
    PipelineBuilder,
    Task,
)
from hls_nextgen_orchestration.metrics import MetricsCollector, _MetricsContext

LOG_GROUP = "/hls/nextgen/test"
LOG_STREAM = "test-job"

A = Asset("A", str)
B = Asset("B", str)


# ----- Helpers


def simple_source(provides: Assets, instrument: bool = False) -> type[DataSource]:
    return make_dataclass(
        "Source",
        [("name", str)],
        bases=(DataSource,),
        namespace={
            "requires": (),
            "provides": provides,
            "instrument": instrument,
            "fetch": lambda self: {
                asset: f"val_{asset.key}" for asset in self.provides
            },
        },
        frozen=True,
    )


def simple_task(
    requires: Assets, provides: Assets, instrument: bool = False
) -> type[Task]:
    def run(self: Task, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
        return {asset: f"val_{asset.key}" for asset in self.provides}

    return make_dataclass(
        "Task",
        [("name", str)],
        bases=(Task,),
        namespace={
            "requires": requires,
            "provides": provides,
            "instrument": instrument,
            "run": run,
        },
        frozen=True,
    )


# ----- Fixtures


@pytest.fixture
def mock_aws_logs() -> Generator[None, None, None]:
    with mock_aws():
        yield


@pytest.fixture
def logs_client(mock_aws_logs: None) -> CloudWatchLogsClient:
    client: CloudWatchLogsClient = boto3.client("logs", region_name="us-east-1")
    client.create_log_group(logGroupName=LOG_GROUP)
    client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    return client


@pytest.fixture
def metrics_env(
    logs_client: CloudWatchLogsClient, monkeypatch: pytest.MonkeyPatch
) -> CloudWatchLogsClient:
    monkeypatch.setenv("METRIC_LOG_GROUP_NAME", LOG_GROUP)
    monkeypatch.setenv("AWS_BATCH_JOB_ID", LOG_STREAM)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    return logs_client


# ----- MetricsCollector initialization


def test_disabled_without_envvar(
    mock_aws_logs: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("METRIC_LOG_GROUP_NAME", raising=False)
    assert not MetricsCollector().enabled


def test_enabled_with_envvar(metrics_env: CloudWatchLogsClient) -> None:
    assert MetricsCollector().enabled


def test_experiment_dims(
    metrics_env: CloudWatchLogsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HLS_EXPERIMENT_FMASK_VERSION", "v5")
    monkeypatch.setenv("HLS_EXPERIMENT_LASRC_VERSION", "2.1")
    collector = MetricsCollector()
    assert collector._experiment_dims == {"fmask_version": "v5", "lasrc_version": "2.1"}


# ----- collect() — nullcontext vs _MetricsContext


def test_collect_noop_when_disabled(
    mock_aws_logs: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("METRIC_LOG_GROUP_NAME", raising=False)
    node = simple_task(requires=(), provides=(A,), instrument=True)("T1")
    assert isinstance(MetricsCollector().collect(node), nullcontext)


def test_collect_noop_for_uninstrumented_node(
    metrics_env: CloudWatchLogsClient,
) -> None:
    node = simple_task(requires=(), provides=(A,), instrument=False)("T1")
    assert isinstance(MetricsCollector().collect(node), nullcontext)


def test_collect_returns_metrics_context_for_instrumented_node(
    metrics_env: CloudWatchLogsClient,
) -> None:
    node = simple_task(requires=(), provides=(A,), instrument=True)("T1")
    assert isinstance(MetricsCollector().collect(node), _MetricsContext)


def test_collect_instruments_datasource_when_opted_in(
    metrics_env: CloudWatchLogsClient,
) -> None:
    node = simple_source(provides=(A,), instrument=True)("Src")
    assert isinstance(MetricsCollector().collect(node), _MetricsContext)


# ----- _emit — CloudWatch payload


def test_emit_sends_emf_payload(metrics_env: CloudWatchLogsClient) -> None:
    node = simple_task(requires=(), provides=(A,), instrument=True)("T1")

    with MetricsCollector().collect(node):
        pass

    events = metrics_env.get_log_events(
        logGroupName=LOG_GROUP, logStreamName=LOG_STREAM
    )
    assert len(events["events"]) == 1

    record = json.loads(events["events"][0]["message"])
    assert record["task_class"] == "Task"
    assert record["task_name"] == "T1"
    assert record["job_id"] == LOG_STREAM
    assert "runtime_seconds" in record
    assert "peak_memory_mb" in record
    assert "max_cpu_percent" in record
    assert "_aws" in record


def test_emit_includes_experiment_dims(
    metrics_env: CloudWatchLogsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HLS_EXPERIMENT_FMASK_VERSION", "v5")
    node = simple_task(requires=(), provides=(A,), instrument=True)("T1")

    with MetricsCollector().collect(node):
        pass

    events = metrics_env.get_log_events(
        logGroupName=LOG_GROUP, logStreamName=LOG_STREAM
    )
    record = json.loads(events["events"][0]["message"])
    assert record["fmask_version"] == "v5"
    assert "fmask_version" in record["_aws"]["CloudWatchMetrics"][0]["Dimensions"][0]


def test_emit_does_not_raise_on_cloudwatch_error(
    metrics_env: CloudWatchLogsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CloudWatch failure must never propagate out of the context manager."""
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "nonexistent-stream")
    node = simple_task(requires=(), provides=(A,), instrument=True)("T1")

    with MetricsCollector().collect(node):
        pass


# ----- Pipeline integration


def test_pipeline_emits_metrics_for_instrumented_tasks(
    metrics_env: CloudWatchLogsClient,
) -> None:
    src = simple_source(provides=(A,))("Src")
    task = simple_task(requires=(A,), provides=(B,), instrument=True)("T1")

    PipelineBuilder().add(src).add(task).build().run()

    events = metrics_env.get_log_events(
        logGroupName=LOG_GROUP, logStreamName=LOG_STREAM
    )
    assert len(events["events"]) == 1
    assert json.loads(events["events"][0]["message"])["task_name"] == "T1"


def test_pipeline_skips_uninstrumented_tasks(
    metrics_env: CloudWatchLogsClient,
) -> None:
    src = simple_source(provides=(A,))("Src")
    task = simple_task(requires=(A,), provides=(B,), instrument=False)("T1")

    PipelineBuilder().add(src).add(task).build().run()

    events = metrics_env.get_log_events(
        logGroupName=LOG_GROUP, logStreamName=LOG_STREAM
    )
    assert len(events["events"]) == 0


def test_pipeline_runs_correctly_without_metrics(
    mock_aws_logs: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("METRIC_LOG_GROUP_NAME", raising=False)
    src = simple_source(provides=(A,))("Src")
    task = simple_task(requires=(A,), provides=(B,), instrument=True)("T1")

    context = PipelineBuilder().add(src).add(task).build().run()

    assert context.get(B) == "val_B"
