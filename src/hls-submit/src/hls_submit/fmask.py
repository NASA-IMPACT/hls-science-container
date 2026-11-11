"""Submit standalone Fmask jobs."""

from __future__ import annotations

import click

from .common import JobSpec, SubmitOptions, job_name, submit_jobs, submit_options

COMMAND = "hls-workflows fmask"
DEFAULT_S30_INPUT_PREFIX = "datasets/belmanip2/S30/"


def build_jobs(options: SubmitOptions) -> list[JobSpec]:
    """One `hls-workflows fmask` job per granule."""
    base = options.base_environment(DEFAULT_S30_INPUT_PREFIX)
    return [
        JobSpec(
            name=job_name("fmask", *options.experiment_labels, l1_id),
            environment={**base, "GRANULE": l1_id},
            granule=l1_id,
        )
        for l1_id in options.read_granules()
    ]


@click.command()
@submit_options
def fmask(options: SubmitOptions) -> None:
    """Submit Fmask jobs for the --sensor granules in CSV_FILE.

    Outputs land in s3://<debug bucket>/fmask/<fmask version>/<granule>/, where
    the version is the fmask package baked into the job definition's image.
    """
    submit_jobs(options, COMMAND, build_jobs(options))
