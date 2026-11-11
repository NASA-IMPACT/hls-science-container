"""Shared options and helpers for building and submitting AWS Batch jobs."""

from __future__ import annotations

import csv
import functools
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import click
from botocore.config import Config

from .resources import (
    ResourceLookupError,
    find_job_definition,
    find_job_queue,
    require_unversioned,
)

if TYPE_CHECKING:
    from mypy_boto3_batch import BatchClient
    from mypy_boto3_cloudformation import CloudFormationClient

DEFAULT_REGION = "us-west-2"
DEFAULT_OUTPUT_BUCKET = "hls-science-container-testing"
EXPERIMENT_PREFIX = "HLS_EXPERIMENT_"


@dataclass(frozen=True)
class Sensor:
    """Batch resources (by CDK construct name) and default inputs for a sensor."""

    job_queue: str
    job_definition: str
    input_bucket: str


SENSORS: dict[str, Sensor] = {
    "L30": Sensor(
        job_queue="BatchLandsatAcJobQueue",
        job_definition="LandsatTaskBatchJob",
        input_bucket="usgs-landsat",
    ),
    "S30": Sensor(
        job_queue="BatchSentinelJobQueue",
        job_definition="SentinelTaskBatchJob",
        input_bucket="hls-science-container-testing",
    ),
}


def batch_client(region: str, max_pool_connections: int = 10) -> BatchClient:
    """Batch client; size ``max_pool_connections`` to the number of threads using it."""
    config = Config(
        max_pool_connections=max_pool_connections,
        retries={"mode": "adaptive", "max_attempts": 10},
    )
    return boto3.client("batch", region_name=region, config=config)


def cloudformation_client(region: str) -> CloudFormationClient:
    return boto3.client("cloudformation", region_name=region)


# ----- Granule input
_L1_ID_SENSORS = (
    (re.compile(r"^LC0[89]_"), "L30"),
    (re.compile(r"^S2[A-D]_MSIL1C_"), "S30"),
)


def clean_l1_id(raw: str) -> str:
    """Normalize an L1 ID cell to the GRANULE value the workflows expect.

    Twin Sentinel-2 granules are listed as "A+B"; only the first is used.
    """
    return raw.split("+")[0].strip().removesuffix(".SAFE")


def l1_sensor(l1_id: str) -> str | None:
    """Return the HLS product ("L30" or "S30") built from an L1 ID, if known."""
    for pattern, sensor in _L1_ID_SENSORS:
        if pattern.match(l1_id):
            return sensor
    return None


def _l1_values(csv_file: Path, column: str | None) -> list[str]:
    """Return the raw L1 ID cells of a CSV, in row order.

    A single-column file with no header (one L1 ID per line) is also accepted.
    """
    with open(csv_file, newline="") as fh:
        table = [row for row in csv.reader(fh) if any(c.strip() for c in row)]
    if not table:
        return []

    header, *body = table
    if column is None and len(header) == 1 and l1_sensor(clean_l1_id(header[0])):
        return [row[0] for row in table]

    if column is None:
        if len(header) != 1:
            raise click.ClickException(
                f"{csv_file} has columns {', '.join(header)}; pass --granule-column "
                "to name the one holding L1 granule IDs."
            )
        index = 0
    elif column in header:
        index = header.index(column)
    else:
        raise click.ClickException(
            f"{csv_file} has no column {column!r}; found {', '.join(header)}."
        )
    return [row[index] for row in body]


def read_granules(
    csv_file: Path,
    sensor: str,
    column: str | None = None,
    row_offset: int = 0,
    rows: int | None = None,
) -> list[str]:
    """Read the L1 granule IDs for ``sensor`` from a CSV."""
    l1_ids = [clean_l1_id(v) for v in _l1_values(csv_file, column)][row_offset:]

    selected = []
    for l1_id in l1_ids:
        found = l1_sensor(l1_id)
        if found is None:
            raise click.ClickException(
                f"{l1_id!r} in {csv_file} is not a Landsat or Sentinel-2 L1 granule "
                "ID. Is --granule-column the L1 input, not the HLS output?"
            )
        if found == sensor:
            selected.append(l1_id)
    return selected if rows is None else selected[:rows]


def job_name(*parts: str | None) -> str:
    """Join non-empty parts into a valid Batch job name ([A-Za-z0-9_-], <= 128)."""
    stem = "-".join(p for p in parts if p)
    return re.sub(r"[^A-Za-z0-9_-]", "-", stem)[:128]


def _parse_experiments(
    ctx: click.Context, param: click.Parameter, values: tuple[str, ...]
) -> dict[str, str]:
    experiments: dict[str, str] = {}
    for value in values:
        key, sep, val = value.partition("=")
        if not sep or not re.fullmatch(r"[A-Za-z0-9_]+", key) or not val:
            raise click.BadParameter(f"expected KEY=VALUE, got {value!r}")
        experiments[key.upper()] = val
    return experiments


# ----- Shared CLI options
stack_name_option = click.option(
    "--stack-name",
    envvar="HLS_STACK_NAME",
    default=None,
    help=(
        "hls-orchestration CloudFormation stack to look up Batch resources in "
        "(env: HLS_STACK_NAME)."
    ),
)
region_option = click.option(
    "--region", default=DEFAULT_REGION, show_default=True, help="AWS region."
)


@dataclass(frozen=True, kw_only=True)
class SubmitOptions:
    """Options shared by every job-submitting command."""

    csv_file: Path
    granule_column: str | None
    sensor: str
    rows: int | None
    row_offset: int
    experiments: dict[str, str]
    stack_name: str | None
    region: str
    job_queue: str | None
    job_definition: str | None
    input_bucket: str | None
    input_prefix: str | None
    requester_pays: bool
    output_bucket: str
    debug_bucket: str | None
    metric_log_group: str | None
    threads: int
    submit: bool

    @property
    def sensor_config(self) -> Sensor:
        return SENSORS[self.sensor]

    @property
    def metric_log_group_name(self) -> str | None:
        if self.metric_log_group:
            return self.metric_log_group
        if self.stack_name:
            return f"/hls-orchestration/{self.stack_name}/log-metrics"
        return None

    @property
    def experiment_labels(self) -> list[str]:
        """Experiment values, in the order given, for use in job names."""
        return list(self.experiments.values())

    def read_granules(self) -> list[str]:
        return read_granules(
            self.csv_file, self.sensor, self.granule_column, self.row_offset, self.rows
        )

    def base_environment(self, default_s30_input_prefix: str = "") -> dict[str, str]:
        """Container environment common to every job of a submission."""
        env = {
            "AWS_DEFAULT_REGION": self.region,
            "INPUT_BUCKET": self.input_bucket or self.sensor_config.input_bucket,
            "OUTPUT_BUCKET": self.output_bucket,
            "DEBUG_BUCKET": self.debug_bucket or self.output_bucket,
            # Required by the Sentinel environment config, unused by experiments.
            "GIBS_OUTPUT_BUCKET": "foo",
            "OMP_NUM_THREADS": str(self.threads),
        }
        if self.metric_log_group_name:
            env["METRIC_LOG_GROUP_NAME"] = self.metric_log_group_name
        input_prefix = self.input_prefix
        if input_prefix is None and self.sensor == "S30":
            input_prefix = default_s30_input_prefix
        if input_prefix:
            env["INPUT_PREFIX"] = input_prefix
        if self.requester_pays:
            env["INPUT_REQUESTER_PAYS"] = "true"
        for key, value in self.experiments.items():
            env[f"{EXPERIMENT_PREFIX}{key}"] = value
        return env

    def resolve_batch_targets(self) -> tuple[str, str]:
        """Return the (job queue, job definition) to submit to."""
        try:
            if self.job_definition is not None:
                require_unversioned(self.job_definition)
            if self.job_queue is not None and self.job_definition is not None:
                return self.job_queue, self.job_definition
            if self.stack_name is None:
                raise click.UsageError(
                    "Pass --stack-name (or set HLS_STACK_NAME) to look up Batch "
                    "resources, or pass both --job-queue and --job-definition."
                )

            cfn = cloudformation_client(self.region)
            queue = self.job_queue or find_job_queue(
                cfn, self.stack_name, self.sensor_config.job_queue
            )
            definition = self.job_definition or find_job_definition(
                cfn, self.stack_name, self.sensor_config.job_definition
            )
        except ResourceLookupError as err:
            raise click.ClickException(str(err)) from err
        return queue, definition


_SUBMIT_OPTION_NAMES = [f.name for f in fields(SubmitOptions)]

_SUBMIT_OPTION_DECORATORS: list[Callable[[Callable[..., Any]], Callable[..., Any]]] = [
    click.argument(
        "csv_file", type=click.Path(exists=True, dir_okay=False, path_type=Path)
    ),
    click.option(
        "--granule-column",
        default=None,
        help=(
            "CSV column holding the L1 input granule IDs. Optional for a "
            "single-column file, with or without a header."
        ),
    ),
    click.option(
        "--sensor",
        type=click.Choice(list(SENSORS)),
        required=True,
        help="Submit only this sensor's granules, by L1 ID (LC08/LC09 or S2A-D).",
    ),
    click.option(
        "--rows",
        type=int,
        default=None,
        help="Submit at most this many granules (after filtering). Default: all.",
    ),
    click.option(
        "--row-offset", type=int, default=0, help="Skip this many CSV rows first."
    ),
    click.option(
        "--experiment",
        "experiments",
        multiple=True,
        callback=_parse_experiments,
        metavar="KEY=VALUE",
        help=(
            f"Set {EXPERIMENT_PREFIX}<KEY>=VALUE, a CloudWatch metrics dimension. "
            "Values are also added to job names. Repeatable."
        ),
    ),
    stack_name_option,
    region_option,
    click.option(
        "--job-queue",
        default=None,
        help="Job queue name or ARN. Default: looked up from the stack.",
    ),
    click.option(
        "--job-definition",
        default=None,
        help=(
            "Job definition name or ARN, without a revision. "
            "Default: looked up from the stack."
        ),
    ),
    click.option(
        "--input-bucket",
        default=None,
        help="Bucket holding the input granules. Default: per --sensor.",
    ),
    click.option(
        "--input-prefix",
        default=None,
        help="Key prefix of the Sentinel-2 granule ZIPs in --input-bucket.",
    ),
    click.option(
        "--requester-pays",
        is_flag=True,
        default=False,
        help=(
            "Read Sentinel-2 inputs from a requester-pays bucket owned by another "
            "account (sets INPUT_REQUESTER_PAYS=true). Landsat always pays."
        ),
    ),
    click.option(
        "--output-bucket",
        default=DEFAULT_OUTPUT_BUCKET,
        show_default=True,
        help="Bucket for job outputs.",
    ),
    click.option(
        "--debug-bucket",
        default=None,
        help="Bucket for debug outputs. Default: --output-bucket.",
    ),
    click.option(
        "--metric-log-group",
        default=None,
        help=(
            "Metrics log group. Default: /hls-orchestration/<stack>/log-metrics "
            "with --stack-name, otherwise metrics are not recorded."
        ),
    ),
    click.option(
        "--threads",
        type=click.IntRange(min=1),
        default=2,
        show_default=True,
        help=(
            "Worker threads per job. Batch allocates vCPUs via cpu shares, so an "
            "unconstrained job sees every core on the instance."
        ),
    ),
    click.option(
        "--submit",
        is_flag=True,
        default=False,
        help="Actually submit jobs. Without this flag the run is a dry run.",
    ),
]


def submit_options(func: Callable[..., None]) -> Callable[..., None]:
    """Add the shared submit options, passed to ``func`` as a ``SubmitOptions``."""

    @functools.wraps(func)
    def wrapper(**kwargs: Any) -> None:
        options = SubmitOptions(**{k: kwargs.pop(k) for k in _SUBMIT_OPTION_NAMES})
        func(options, **kwargs)

    decorated: Callable[..., Any] = wrapper
    for decorator in reversed(_SUBMIT_OPTION_DECORATORS):
        decorated = decorator(decorated)
    return decorated


# ----- Submission
@dataclass(frozen=True)
class JobSpec:
    """A single job to submit."""

    name: str
    environment: dict[str, str]
    granule: str


def submit_jobs(options: SubmitOptions, command: str, jobs: Sequence[JobSpec]) -> None:
    """Submit ``jobs`` running ``command``, or print them on a dry run."""
    job_queue, job_definition = options.resolve_batch_targets()

    mode = "SUBMIT" if options.submit else "DRY-RUN"
    experiments = (
        ", ".join(f"{k}={v}" for k, v in options.experiments.items()) or "none"
    )
    metrics = options.metric_log_group_name or "not recorded"
    click.echo(
        f"[{mode}] sensor={options.sensor} {len(jobs)} job(s)\n"
        f"  queue={job_queue}\n"
        f"  job definition={job_definition}\n"
        f"  experiments={experiments}\n"
        f"  metrics={metrics}"
    )

    batch = batch_client(options.region) if options.submit else None
    for job in jobs:
        if batch is None:
            click.echo(f"  would submit {job.name} (GRANULE={job.granule})")
            continue
        resp = batch.submit_job(
            jobName=job.name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            containerOverrides={
                "command": [command],
                "environment": [
                    {"name": k, "value": v} for k, v in job.environment.items()
                ],
            },
        )
        click.echo(f"  submitted {job.name} -> {resp['jobId']}")

    if batch is None:
        click.echo("Re-run with --submit to actually submit these jobs.")
