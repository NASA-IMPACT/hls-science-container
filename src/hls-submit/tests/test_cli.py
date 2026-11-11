from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber
from click.testing import CliRunner

from hls_submit.cli import cli
from hls_submit.terminate import terminate_jobs

CSV_ARGS = ["--granule-column", "L1_ID"]


def _stub_stack_lookups(
    aws: dict[str, Stubber], stack_name: str, response: dict[str, Any], count: int = 2
) -> None:
    for _ in range(count):
        aws["cloudformation"].add_response(
            "list_stack_resources", response, {"StackName": stack_name}
        )


def test_fmask_dry_run_looks_up_stack(
    aws: dict[str, Stubber],
    stack: dict[str, str],
    stack_resources_response: dict[str, Any],
    granules_csv: Path,
) -> None:
    _stub_stack_lookups(aws, stack["name"], stack_resources_response)

    result = CliRunner().invoke(
        cli,
        [
            "fmask",
            str(granules_csv),
            *CSV_ARGS,
            "--sensor",
            "S30",
            "--stack-name",
            stack["name"],
        ],
    )

    assert result.exit_code == 0, result.output
    assert "[DRY-RUN] sensor=S30 2 job(s)" in result.output
    assert f"queue={stack['sentinel_queue']}\n" in result.output
    assert f"job definition={stack['sentinel_job_definition']}\n" in result.output
    assert "would submit fmask-S2A_MSIL1C" in result.output


def test_fmask_submit_requester_pays(
    aws: dict[str, Stubber],
    stack: dict[str, str],
    stack_resources_response: dict[str, Any],
    granules_csv: Path,
) -> None:
    _stub_stack_lookups(aws, stack["name"], stack_resources_response)
    granule = "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
    expected_env = {
        "AWS_DEFAULT_REGION": "us-west-2",
        "INPUT_BUCKET": "hls-validation-datasets",
        "OUTPUT_BUCKET": "hls-science-container-testing",
        "DEBUG_BUCKET": "hls-science-container-testing",
        "GIBS_OUTPUT_BUCKET": "foo",
        "OMP_NUM_THREADS": "2",
        "RAYON_NUM_THREADS": "2",
        "METRIC_LOG_GROUP_NAME": f"/hls-orchestration/{stack['name']}/log-metrics",
        "INPUT_PREFIX": "datasets/belmanip2/S30/",
        "INPUT_REQUESTER_PAYS": "true",
        "HLS_EXPERIMENT_FMASK_BUILD": "baseline",
        "GRANULE": granule,
    }
    aws["batch"].add_response(
        "submit_job",
        {"jobName": "x", "jobId": "job-1"},
        {
            "jobName": f"fmask-baseline-{granule}",
            "jobQueue": stack["sentinel_queue"],
            "jobDefinition": stack["sentinel_job_definition"],
            "containerOverrides": {
                "command": ["hls-workflows fmask"],
                "environment": [
                    {"name": k, "value": v} for k, v in expected_env.items()
                ],
            },
        },
    )

    result = CliRunner().invoke(
        cli,
        [
            "fmask",
            str(granules_csv),
            *CSV_ARGS,
            "--sensor",
            "S30",
            "--stack-name",
            stack["name"],
            "--rows",
            "1",
            "--input-bucket",
            "hls-validation-datasets",
            "--requester-pays",
            "--experiment",
            "fmask_build=baseline",
            "--submit",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "-> job-1" in result.output


def test_lasrc_overrides_skip_lookup(
    aws: dict[str, Stubber], granules_csv: Path
) -> None:
    for impl in ("c", "rust"):
        aws["batch"].add_response(
            "submit_job",
            {"jobName": "x", "jobId": f"job-{impl}"},
            {
                "jobName": f"lasrc-{impl}-LC08_L1TP_044032_20250111_20250111_02_RT",
                "jobQueue": "my-queue",
                "jobDefinition": "my-def",
                "containerOverrides": {
                    "command": ["hls-workflows lasrc"],
                    "environment": ANY,
                },
            },
        )

    result = CliRunner().invoke(
        cli,
        [
            "lasrc",
            str(granules_csv),
            *CSV_ARGS,
            "--sensor",
            "L30",
            "--job-queue",
            "my-queue",
            "--job-definition",
            "my-def",
            "--submit",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "-> job-c" in result.output
    assert "-> job-rust" in result.output


def test_job_definition_revision_rejected(
    aws: dict[str, Stubber], granules_csv: Path
) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "fmask",
            str(granules_csv),
            *CSV_ARGS,
            "--sensor",
            "S30",
            "--job-queue",
            "my-queue",
            "--job-definition",
            "my-def:3",
        ],
    )

    assert result.exit_code != 0
    assert "pinned to a revision" in result.output


def test_terminate_dry_run(
    aws: dict[str, Stubber],
    stack: dict[str, str],
    stack_resources_response: dict[str, Any],
) -> None:
    _stub_stack_lookups(aws, stack["name"], stack_resources_response, count=1)
    for state in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
        summaries = (
            [{"jobId": "job-1", "jobName": "fmask-a", "jobArn": "arn"}]
            if state == "RUNNING"
            else []
        )
        aws["batch"].add_response(
            "list_jobs",
            {"jobSummaryList": summaries},
            {"jobQueue": stack["sentinel_queue"], "jobStatus": state},
        )

    result = CliRunner().invoke(
        cli, ["terminate", "--sensor", "S30", "--stack-name", stack["name"]]
    )

    assert result.exit_code == 0, result.output
    assert "[RUNNING  ] job-1  fmask-a" in result.output
    assert "Dry run." in result.output


def test_terminate_requires_target(aws: dict[str, Stubber]) -> None:
    result = CliRunner().invoke(cli, ["terminate"])

    assert result.exit_code != 0
    assert "Pass --job-queue, or --sensor with --stack-name" in result.output


def test_lookup_requires_stack_name(
    aws: dict[str, Stubber], granules_csv: Path
) -> None:
    result = CliRunner().invoke(
        cli, ["fmask", str(granules_csv), *CSV_ARGS, "--sensor", "S30"]
    )

    assert result.exit_code != 0
    assert "Pass --stack-name (or set HLS_STACK_NAME)" in result.output


def test_overrides_without_stack_skip_metrics(
    aws: dict[str, Stubber], granules_csv: Path
) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "fmask",
            str(granules_csv),
            *CSV_ARGS,
            "--sensor",
            "S30",
            "--job-queue",
            "my-queue",
            "--job-definition",
            "my-def",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "metrics=not recorded" in result.output


def test_bad_experiment_rejected(aws: dict[str, Stubber], granules_csv: Path) -> None:
    result = CliRunner().invoke(
        cli,
        ["fmask", str(granules_csv), "--sensor", "S30", "--experiment", "nope"],
    )

    assert result.exit_code != 0
    assert "expected KEY=VALUE" in result.output


def test_aws_error_reported_without_traceback(
    aws: dict[str, Stubber], stack: dict[str, str], granules_csv: Path
) -> None:
    aws["cloudformation"].add_client_error(
        "list_stack_resources",
        service_error_code="ExpiredToken",
        service_message="The security token included in the request is expired",
    )

    result = CliRunner().invoke(
        cli,
        [
            "fmask",
            str(granules_csv),
            *CSV_ARGS,
            "--sensor",
            "S30",
            "--stack-name",
            stack["name"],
        ],
    )

    assert result.exit_code == 1
    assert "AWS error:" in result.output
    assert "ExpiredToken" in result.output


class _FakeBatch:
    """Thread-safe stand-in for terminate_job; fails for job IDs in ``fail``."""

    def __init__(self, fail: set[str]) -> None:
        self.fail = fail
        self.calls: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def terminate_job(self, *, jobId: str, reason: str) -> dict[str, Any]:
        with self._lock:
            self.calls.append((jobId, reason))
        if jobId in self.fail:
            raise ClientError(
                {"Error": {"Code": "ClientException", "Message": "nope"}},
                "TerminateJob",
            )
        return {}


def test_terminate_jobs_concurrently_collects_failures() -> None:
    jobs = [(f"job-{i}", f"name-{i}", "RUNNING") for i in range(20)]
    batch = _FakeBatch(fail={"job-3", "job-11"})

    failures = terminate_jobs(batch, jobs, "why", workers=4)  # type: ignore[arg-type]

    assert sorted(job_id for job_id, _ in batch.calls) == sorted(j[0] for j in jobs)
    assert all(reason == "why" for _, reason in batch.calls)
    assert sorted(f[0] for f in failures) == ["job-11", "job-3"]
