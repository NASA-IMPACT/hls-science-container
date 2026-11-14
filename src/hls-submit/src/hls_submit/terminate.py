"""Terminate active jobs in a job queue."""

from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import click
from botocore.exceptions import BotoCoreError, ClientError

from .common import (
    SENSORS,
    batch_client,
    cloudformation_client,
    region_option,
    stack_name_option,
)
from .resources import ResourceLookupError, find_job_queue

if TYPE_CHECKING:
    from mypy_boto3_batch import BatchClient
    from mypy_boto3_batch.literals import JobStatusType

# Job states that can still be terminated; SUCCEEDED and FAILED are done.
ACTIVE_STATES: tuple[JobStatusType, ...] = (
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
)


def list_active_jobs(batch: BatchClient, job_queue: str) -> list[tuple[str, str, str]]:
    """Return (job ID, job name, state) for every active job in the queue."""
    jobs: list[tuple[str, str, str]] = []
    paginator = batch.get_paginator("list_jobs")
    for state in ACTIVE_STATES:
        for page in paginator.paginate(jobQueue=job_queue, jobStatus=state):
            for job in page["jobSummaryList"]:
                jobs.append((job["jobId"], job.get("jobName", ""), state))
    return jobs


def terminate_jobs(
    batch: BatchClient,
    jobs: Sequence[tuple[str, str, str]],
    reason: str,
    workers: int,
) -> list[tuple[str, str, str]]:
    """Terminate ``jobs`` concurrently; return (job ID, job name, error) failures."""

    def _terminate(job_id: str) -> None:
        batch.terminate_job(jobId=job_id, reason=reason)

    failures: list[tuple[str, str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_terminate, job_id): (job_id, name) for job_id, name, _ in jobs
        }
        for done, future in enumerate(as_completed(futures), start=1):
            job_id, name = futures[future]
            try:
                future.result()
            except (BotoCoreError, ClientError) as err:
                failures.append((job_id, name, str(err)))
                click.echo(f"  [{done}/{len(jobs)}] FAILED {job_id}  {name}: {err}")
            else:
                click.echo(f"  [{done}/{len(jobs)}] terminated {job_id}  {name}")
    return failures


@click.command()
@click.option(
    "--sensor",
    type=click.Choice(list(SENSORS)),
    default=None,
    help="Look up this sensor's job queue from the stack.",
)
@click.option(
    "--job-queue", default=None, help="Job queue name or ARN. Overrides --sensor."
)
@stack_name_option
@region_option
@click.option(
    "--reason",
    default="Terminated by hls-submit",
    show_default=True,
    help="Reason recorded on each terminated job.",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=16,
    show_default=True,
    help="Number of concurrent terminate_job calls.",
)
@click.option(
    "--terminate",
    is_flag=True,
    default=False,
    help="Actually terminate the jobs. Without this flag the run is a dry run.",
)
def terminate(
    sensor: str | None,
    job_queue: str | None,
    stack_name: str | None,
    region: str,
    reason: str,
    workers: int,
    terminate: bool,
) -> None:
    """Terminate all active jobs in a job queue.

    terminate_job cancels jobs that have not started and stops running ones, so
    one call covers every active state.
    """
    if job_queue is None:
        if sensor is None or stack_name is None:
            raise click.UsageError(
                "Pass --job-queue, or --sensor with --stack-name (or HLS_STACK_NAME)."
            )
        try:
            job_queue = find_job_queue(
                cloudformation_client(region), stack_name, SENSORS[sensor].job_queue
            )
        except ResourceLookupError as err:
            raise click.ClickException(str(err)) from err

    batch = batch_client(region, max_pool_connections=workers)
    jobs = list_active_jobs(batch, job_queue)
    if not jobs:
        click.echo(f"No active jobs found in queue {job_queue}.")
        return

    click.echo(f"Found {len(jobs)} active job(s) in queue {job_queue}:")
    for job_id, name, state in jobs:
        click.echo(f"  [{state:9}] {job_id}  {name}")

    if not terminate:
        click.echo("Dry run. Re-run with --terminate to terminate these jobs.")
        return

    failures = terminate_jobs(batch, jobs, reason, workers)
    click.echo(f"Terminated {len(jobs) - len(failures)} of {len(jobs)} job(s).")
    if failures:
        raise click.ClickException(
            f"{len(failures)} job(s) failed to terminate: "
            + ", ".join(job_id for job_id, _, _ in failures)
        )
