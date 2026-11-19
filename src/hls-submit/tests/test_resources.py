from __future__ import annotations

from typing import Any

import boto3
import pytest
from botocore.stub import Stubber

from hls_submit.resources import (
    ResourceLookupError,
    find_job_definition,
    find_job_queue,
    require_unversioned,
    strip_revision,
)

JOB_DEF_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/SentinelTask-92a4"


@pytest.fixture
def cfn(
    aws: dict[str, Stubber],
    stack: dict[str, str],
    stack_resources_response: dict[str, Any],
) -> Any:
    aws["cloudformation"].add_response(
        "list_stack_resources", stack_resources_response, {"StackName": stack["name"]}
    )
    return boto3.client("cloudformation")


def test_find_job_queue_ignores_similar_names(cfn: Any, stack: dict[str, str]) -> None:
    """BatchSentinelJobQueue must not also match BatchSentinelHistoricJobQueue."""
    queue = find_job_queue(cfn, stack["name"], "BatchSentinelJobQueue")

    assert queue == stack["sentinel_queue"]


def test_find_job_definition_strips_revision(cfn: Any, stack: dict[str, str]) -> None:
    definition = find_job_definition(cfn, stack["name"], "SentinelTaskBatchJob")

    assert definition == stack["sentinel_job_definition"]


def test_find_job_queue_missing(cfn: Any, stack: dict[str, str]) -> None:
    with pytest.raises(ResourceLookupError, match="found 0. Available: Batch"):
        find_job_queue(cfn, stack["name"], "BatchLaadsJobQueue")


@pytest.mark.parametrize("job_definition", [f"{JOB_DEF_ARN}:54", "SentinelTask:3"])
def test_require_unversioned_rejects_revision(job_definition: str) -> None:
    with pytest.raises(ResourceLookupError, match="pinned to a revision"):
        require_unversioned(job_definition)


@pytest.mark.parametrize("job_definition", [JOB_DEF_ARN, "SentinelTask"])
def test_require_unversioned_accepts(job_definition: str) -> None:
    assert require_unversioned(job_definition) == job_definition
    assert strip_revision(job_definition) == job_definition
