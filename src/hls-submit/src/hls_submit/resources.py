"""Look up AWS Batch resources deployed by an hls-orchestration stack."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_cloudformation import CloudFormationClient

JOB_QUEUE_TYPE = "AWS::Batch::JobQueue"
JOB_DEFINITION_TYPE = "AWS::Batch::JobDefinition"

_REVISION = re.compile(r":\d+$")


class ResourceLookupError(Exception):
    """A stack resource could not be resolved to exactly one match."""


def _logical_id_pattern(name: str) -> re.Pattern[str]:
    # CDK logical IDs are the construct path plus an 8 hex character hash, so
    # anchoring on the hash keeps e.g. "BatchSentinelJobQueue" from matching
    # "BatchSentinelHistoricJobQueue".
    return re.compile(rf"^{re.escape(name)}[0-9A-F]{{8}}$")


def find_stack_resource(
    cfn: CloudFormationClient, stack_name: str, resource_type: str, name: str
) -> str:
    """Return the physical ID of the ``resource_type`` named ``name`` in a stack.

    ``name`` is the CDK construct name, i.e. the logical ID without its hash
    suffix (e.g. "BatchSentinelJobQueue").
    """
    pattern = _logical_id_pattern(name)
    available: list[str] = []
    matches: list[str] = []

    paginator = cfn.get_paginator("list_stack_resources")
    for page in paginator.paginate(StackName=stack_name):
        for resource in page["StackResourceSummaries"]:
            if resource["ResourceType"] != resource_type:
                continue
            logical_id = resource["LogicalResourceId"]
            available.append(logical_id)
            if pattern.match(logical_id) and resource.get("PhysicalResourceId"):
                matches.append(resource["PhysicalResourceId"])

    if len(matches) != 1:
        raise ResourceLookupError(
            f"Expected one {resource_type} named {name!r} in stack {stack_name!r}, "
            f"found {len(matches)}. Available: {', '.join(sorted(available)) or 'none'}"
        )
    return matches[0]


def strip_revision(job_definition: str) -> str:
    """Drop a trailing ``:<revision>`` from a job definition name or ARN."""
    return _REVISION.sub("", job_definition)


def require_unversioned(job_definition: str) -> str:
    """Reject job definitions pinned to a revision."""
    if _REVISION.search(job_definition):
        raise ResourceLookupError(
            f"Job definition {job_definition!r} is pinned to a revision. Pass it "
            f"without one ({strip_revision(job_definition)!r}) so Batch uses the "
            "latest ACTIVE revision."
        )
    return job_definition


def find_job_queue(cfn: CloudFormationClient, stack_name: str, name: str) -> str:
    """Return the ARN of a job queue in the stack."""
    return find_stack_resource(cfn, stack_name, JOB_QUEUE_TYPE, name)


def find_job_definition(cfn: CloudFormationClient, stack_name: str, name: str) -> str:
    """Return the revision-less ARN of a job definition in the stack."""
    return strip_revision(
        find_stack_resource(cfn, stack_name, JOB_DEFINITION_TYPE, name)
    )
