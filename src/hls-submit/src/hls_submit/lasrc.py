"""Submit LaSRC jobs, one per granule and implementation."""

from __future__ import annotations

import click

from .common import (
    EXPERIMENT_PREFIX,
    JobSpec,
    SubmitOptions,
    job_name,
    submit_jobs,
    submit_options,
)

COMMAND = "hls-workflows lasrc"
DEFAULT_S30_INPUT_PREFIX = "inputs/lasrc-rs/S30"
LASRC_IMPLS = ("c", "rust")


def build_jobs(options: SubmitOptions, impls: tuple[str, ...]) -> list[JobSpec]:
    """One `hls-workflows lasrc` job per granule per implementation."""
    base = {
        **options.base_environment(DEFAULT_S30_INPUT_PREFIX),
        "VIIRS_AUX_STARTING_DATE": "20210101",
        "LASRC_AUX_DIR": "/var/lasrc_aux",
        # OMP_NUM_THREADS caps the C OpenMP loops; the Rust rayon pool reads
        # RAYON_NUM_THREADS instead.
        "RAYON_NUM_THREADS": str(options.threads),
    }
    return [
        JobSpec(
            name=job_name("lasrc", *options.experiment_labels, impl, l1_id),
            environment={
                **base,
                "GRANULE": l1_id,
                "LASRC_IMPL": impl,
                f"{EXPERIMENT_PREFIX}LASRC_IMPL": impl,
            },
            granule=l1_id,
        )
        for l1_id in options.read_granules()
        for impl in impls
    ]


@click.command()
@submit_options
@click.option(
    "--lasrc-impl",
    "impls",
    type=click.Choice(LASRC_IMPLS),
    multiple=True,
    default=LASRC_IMPLS,
    show_default=True,
    help=(
        "LaSRC implementation(s) to submit per granule (repeatable). Each job is "
        f"tagged {EXPERIMENT_PREFIX}LASRC_IMPL=<impl>."
    ),
)
def lasrc(options: SubmitOptions, impls: tuple[str, ...]) -> None:
    """Submit LaSRC jobs for the --sensor granules in CSV_FILE."""
    submit_jobs(options, COMMAND, build_jobs(options, impls))
