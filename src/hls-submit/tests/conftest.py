from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber

STACK_NAME = "hls-test-stack"
ACCOUNT_ARN = "arn:aws:batch:us-west-2:123456789012"
SENTINEL_QUEUE_ARN = f"{ACCOUNT_ARN}:job-queue/BatchSentinelJobQueue2E4-abc"
SENTINEL_HISTORIC_QUEUE_ARN = f"{ACCOUNT_ARN}:job-queue/BatchSentinelHistoricJob-def"
LANDSAT_QUEUE_ARN = f"{ACCOUNT_ARN}:job-queue/BatchLandsatAcJobQueueD5-ghi"
SENTINEL_JOB_DEF_ARN = f"{ACCOUNT_ARN}:job-definition/SentinelTaskBatchJobDE3-92a4"
LANDSAT_JOB_DEF_ARN = f"{ACCOUNT_ARN}:job-definition/LandsatTaskBatchJob1274-d156"


def _resource(resource_type: str, logical_id: str, physical_id: str) -> dict[str, Any]:
    return {
        "ResourceType": resource_type,
        "LogicalResourceId": logical_id,
        "PhysicalResourceId": physical_id,
        "LastUpdatedTimestamp": "2026-01-01T00:00:00Z",
        "ResourceStatus": "UPDATE_COMPLETE",
    }


STACK_RESOURCES = [
    _resource(
        "AWS::Batch::ComputeEnvironment",
        "BatchComputeEnvironment97329F07",
        f"{ACCOUNT_ARN}:compute-environment/BatchComputeEnvironment9-xyz",
    ),
    _resource(
        "AWS::Batch::JobQueue", "BatchSentinelJobQueue2E4B750B", SENTINEL_QUEUE_ARN
    ),
    _resource(
        "AWS::Batch::JobQueue",
        "BatchSentinelHistoricJobQueueB5C8120D",
        SENTINEL_HISTORIC_QUEUE_ARN,
    ),
    _resource(
        "AWS::Batch::JobQueue", "BatchLandsatAcJobQueueD5A33C58", LANDSAT_QUEUE_ARN
    ),
    _resource(
        "AWS::Batch::JobDefinition",
        "SentinelTaskBatchJobDE3AA4E9",
        f"{SENTINEL_JOB_DEF_ARN}:54",
    ),
    _resource(
        "AWS::Batch::JobDefinition",
        "LandsatTaskBatchJob12745505",
        f"{LANDSAT_JOB_DEF_ARN}:47",
    ),
]


@pytest.fixture
def stack() -> dict[str, str]:
    """Names and ARNs of the stubbed orchestration stack."""
    return {
        "name": STACK_NAME,
        "sentinel_queue": SENTINEL_QUEUE_ARN,
        "sentinel_job_definition": SENTINEL_JOB_DEF_ARN,
    }


@pytest.fixture
def stack_resources_response() -> dict[str, Any]:
    return {"StackResourceSummaries": STACK_RESOURCES}


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch) -> Generator[dict[str, Stubber]]:
    """Stubbed batch and cloudformation clients returned by ``boto3.client``."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("HLS_STACK_NAME", raising=False)

    clients = {
        service: boto3.client(service, region_name="us-west-2")
        for service in ("batch", "cloudformation")
    }
    stubbers = {service: Stubber(client) for service, client in clients.items()}
    for stubber in stubbers.values():
        stubber.activate()

    monkeypatch.setattr(boto3, "client", lambda service, **_: clients[service])
    yield stubbers

    for stubber in stubbers.values():
        stubber.assert_no_pending_responses()
        stubber.deactivate()


@pytest.fixture
def granules_csv(tmp_path: Path) -> Path:
    path = tmp_path / "granules.csv"
    path.write_text(
        "Tile_ID,Granule_ID,L1_ID\n"
        "10SFJ,HLS.L30.T10SFJ.2025011T184522.v2.0,LC08_L1TP_044032_20250111_20250111_02_RT\n"
        "32TQM,HLS.S30.T32TQM.2020001T102431.v2.0,"
        "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841.SAFE\n"
        "33UUP,HLS.S30.T33UUP.2020002T102431.v2.0,"
        "S2B_MSIL1C_20200102T102431_N0208_R065_T33UUP_20200102T122841"
        "+S2B_MSIL1C_20200102T102431_N0208_R065_T33UUP_20200102T130000\n"
    )
    return path
