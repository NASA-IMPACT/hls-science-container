"""Command line interface: ``hls-validate-metadata``."""

from __future__ import annotations

import json
from pathlib import Path

import click

from hls_metadata_validation.findings import Report
from hls_metadata_validation.golden import golden_dir
from hls_metadata_validation.runner import dump_dir, validate_dir

PRODUCTS = click.Choice(["L30", "S30"])
DIR = click.Path(exists=True, file_okay=False, path_type=Path)


def _finish(report: Report, json_path: Path | None) -> None:
    click.echo(report.to_text())
    if json_path is not None:
        json_path.write_text(json.dumps(report.to_dict(), indent=2) + "\n")
    raise SystemExit(0 if report.ok else 1)


@click.group()
def main() -> None:
    """Validate HLS product metadata against the packaged specs."""


@main.command()
@click.argument("product_dir", type=DIR)
@click.argument("out_dir", type=click.Path(file_okay=False, path_type=Path))
def dump(product_dir: Path, out_dir: Path) -> None:
    """Write normalized metadata of PRODUCT_DIR's outputs into OUT_DIR."""
    written = dump_dir(product_dir, out_dir)
    click.echo(f"wrote {len(written)} file(s) to {out_dir}")


@main.command()
@click.argument("product_dir", type=DIR)
@click.option("--product", type=PRODUCTS, default=None, help="Override product type.")
@click.option(
    "--json", "json_path", type=click.Path(dir_okay=False, path_type=Path), default=None
)
def spec(product_dir: Path, product: str | None, json_path: Path | None) -> None:
    """Validate outputs in PRODUCT_DIR against the product spec."""
    _finish(validate_dir(product_dir, product), json_path)


@main.command()
@click.argument("new_dir", type=DIR)
@click.argument("ref_dir", type=DIR)
@click.option(
    "--json", "json_path", type=click.Path(dir_okay=False, path_type=Path), default=None
)
def golden(new_dir: Path, ref_dir: Path, json_path: Path | None) -> None:
    """Validate NEW_DIR, then compare it against reference outputs in REF_DIR."""
    _finish(golden_dir(new_dir, ref_dir), json_path)
