"""Command-line interface for the HLS NextGen orchestration pipelines.

Each subcommand mirrors a workflow's former ``__main__`` block, but options
default from the same environment variables (via ``envvar=``) so existing
container invocations keep working unchanged.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from hls_nextgen_orchestration.constants import FMASK_VERSION, LASRC_VERSION

if TYPE_CHECKING:
    from hls_nextgen_orchestration.pipeline import Pipeline

logger = logging.getLogger(__name__)


def _fmask_option[F: Callable[..., Any]](func: F) -> F:
    """Shared ``--fmask-version`` option (env: FMASK_VERSION).

    Only Fmask v5 is supported; the option is retained as a backward-compatible
    no-op so existing job definitions that set ``FMASK_VERSION`` do not error.
    """

    def _normalize(
        ctx: click.Context, param: click.Parameter, value: str | None
    ) -> FMASK_VERSION:
        # Only v5 is supported; any value resolves to v5.
        return "v5"

    return click.option(
        "--fmask-version",
        envvar="FMASK_VERSION",
        default="v5",
        callback=_normalize,
        help="Fmask version (only v5 is supported; env: FMASK_VERSION).",
    )(func)


def _lasrc_option[F: Callable[..., Any]](func: F) -> F:
    """Shared ``--lasrc-version`` option (env: LASRC_VERSION, also accepts 'rs')."""

    def _normalize(
        ctx: click.Context, param: click.Parameter, value: str
    ) -> LASRC_VERSION:
        # LASRC_VERSION rust/rs -> rust, anything else -> c
        return "rust" if value.lower() in ("rust", "rs") else "c"

    return click.option(
        "--lasrc-version",
        envvar="LASRC_VERSION",
        default="c",
        required=True,
        callback=_normalize,
        help="LaSRC version: c or rust (env: LASRC_VERSION, also accepts 'rs').",
    )(func)


def _run(pipeline: Pipeline, pipeline_name: str) -> None:
    """Run a pipeline under aggregate metrics and exit with its exit code."""
    try:
        print(pipeline)
        with pipeline.metrics.collect_pipeline(
            pipeline_class="Pipeline", pipeline_name=pipeline_name
        ):
            context = pipeline.run()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise SystemExit(1) from e
    raise SystemExit(context.exit_code)


@click.group()
def cli() -> None:
    """HLS NextGen orchestration pipelines."""
    logging.basicConfig(level=logging.INFO)


@cli.command("sentinel")
@click.option(
    "--granule-list",
    envvar="GRANULE_LIST",
    default=None,
    help="Comma-separated Sentinel-2 granule IDs (env: GRANULE_LIST).",
)
@click.option(
    "--local-granule-zips",
    envvar="LOCAL_GRANULE_ZIPS",
    default=None,
    help="Comma-separated local granule zip paths (env: LOCAL_GRANULE_ZIPS).",
)
@_fmask_option
def sentinel(
    granule_list: str | None,
    local_granule_zips: str | None,
    fmask_version: FMASK_VERSION,
) -> None:
    """Run the Sentinel-2 (S30) preprocessing pipeline."""
    from hls_nextgen_orchestration.sentinel.workflow import construct_pipeline

    granule_ids = granule_list.split(",") if granule_list else None
    zips: list[Path] | None = None
    if local_granule_zips:
        zips = [Path(z) for z in local_granule_zips.split(",")]
        granule_ids = [zf.stem for zf in zips]
        # The Sentinel EnvSource reads GRANULE_LIST from the environment at run time.
        os.environ["GRANULE_LIST"] = ",".join(granule_ids)

    _run(
        construct_pipeline(
            granule_ids=granule_ids,
            local_granule_zips=zips,
            fmask_version=fmask_version,
        ),
        "sentinel-ac",
    )


@cli.command("landsat-ac")
@click.option(
    "--granule",
    envvar="GRANULE",
    required=True,
    help="Landsat granule ID (env: GRANULE).",
)
@click.option(
    "--local-granule-dir",
    envvar="LOCAL_GRANULE_DIR",
    default=None,
    type=click.Path(path_type=Path),
    help="Pre-downloaded Landsat granule directory (env: LOCAL_GRANULE_DIR).",
)
@_fmask_option
def landsat_ac(
    granule: str,
    local_granule_dir: Path | None,
    fmask_version: FMASK_VERSION,
) -> None:
    """Run the Landsat atmospheric correction (L30-AC) pipeline."""
    from hls_nextgen_orchestration.landsat_ac.workflow import construct_pipeline

    _run(
        construct_pipeline(
            granule_id=granule,
            local_granule_dir=local_granule_dir,
            fmask_version=fmask_version,
        ),
        "landsat-ac",
    )


@cli.command("landsat-tile")
@click.option(
    "--local-pathrows-dir",
    envvar="LOCAL_PATHROWS_DIR",
    default=None,
    type=click.Path(path_type=Path),
    help="Pre-downloaded path/row inputs directory (env: LOCAL_PATHROWS_DIR).",
)
def landsat_tile(local_pathrows_dir: Path | None) -> None:
    """Run the Landsat tiling (L30) pipeline."""
    from hls_nextgen_orchestration.landsat_tile.workflow import construct_pipeline

    _run(construct_pipeline(local_pathrows_dir=local_pathrows_dir), "landsat-tile")


@cli.command("lasrc")
@click.option(
    "--granule",
    envvar="GRANULE",
    required=True,
    help="Sentinel-2 or Landsat granule ID, auto-detected (env: GRANULE).",
)
@click.option(
    "--local-granule",
    envvar="LOCAL_GRANULE",
    default=None,
    type=click.Path(path_type=Path),
    help="Pre-downloaded granule (.zip for S2, dir for Landsat) (env: LOCAL_GRANULE).",
)
@_lasrc_option
def lasrc(
    granule: str,
    local_granule: Path | None,
    lasrc_version: LASRC_VERSION,
) -> None:
    """Run the standalone LaSRC pipeline (Download -> LaSRC -> Upload).

    Temporary pipeline for C vs Rust (lasrc-rs) intercomparison. The Rust path
    runs straight off the downloaded scene; the C path runs the ESPA-conversion
    chain it requires. Neither path runs Fmask (LaSRC does not consume it).
    """
    from hls_nextgen_orchestration.lasrc import construct_pipeline

    _run(
        construct_pipeline(
            granule_id=granule,
            lasrc_version=lasrc_version,
            local_granule=local_granule,
        ),
        "lasrc-workflow",
    )


if __name__ == "__main__":
    cli()
