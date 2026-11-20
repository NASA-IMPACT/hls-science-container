#!/usr/bin/env python
"""
Cut small reference fixtures for the LaSRC C vs Rust toggle tests.

Writes these files into --out:

    landsat_c_lasrc.xml           ESPA XML after C LaSRC (Landsat)
    sentinel_c_lasrc.xml          ESPA XML after C LaSRC (Sentinel-2); only
                                  written when --c-sentinel-dir is given
    landsat_toa_bt_window.npz     256x256 window: b9, b10, b11, qa_pixel, sza,
                                  c_toa_band9, c_bt_band10, c_bt_band11
    landsat_rust_sr_window.npz    256x256 window: sr_band4 from the Rust run

Inputs are kept working directories:

    # C Landsat: granule dir holds <granule>.xml and ENVI .img files
    hls-workflows landsat-ac --granule <id> --no-cleanup-working-dir

    # C Sentinel-2: ESPA XML sits in the SAFE dir of the granule working dir
    hls-workflows sentinel --granule-list <id> --no-cleanup-working-dir

    # Rust Landsat: output dir holds <product_id>_sr_band4.img
    hls-workflows lasrc --granule <id> --lasrc-version rust

Band files are located by parsing each ESPA XML (<band name> / <file_name>).

    pixi run -e debug python src/hls-utilities/tests/data/lasrc/make_fixtures.py \\
        --c-landsat-dir <dir> --c-sentinel-dir <dir> --rust-landsat-dir <dir> \\
        --row 4000 --col 0 --out src/hls-utilities/tests/data/lasrc
"""

from __future__ import annotations

import shutil
from pathlib import Path

import click
import numpy as np
import rasterio
from lxml import etree
from rasterio.windows import Window

ESPA_NS = {"e": "http://espa.cr.usgs.gov/v2"}
SIZE = 256

LANDSAT_INPUTS = {
    "b9": (("b9",), "uint16"),
    "b10": (("b10",), "uint16"),
    "b11": (("b11",), "uint16"),
    "qa_pixel": (("qa_pixel",), "uint16"),
    "sza": (("sza",), "int16"),
}
LANDSAT_C_OUTPUTS = {
    "c_toa_band9": (("toa_band9",), "uint16"),
    "c_bt_band10": (("bt_band10",), "uint16"),
    "c_bt_band11": (("bt_band11",), "uint16"),
}


def is_lasrc_espa_xml(path: Path) -> bool:
    """True for an ESPA XML that C LaSRC appended its bands to.

    The HLS steps after LaSRC write derived ESPA XMLs alongside (``*_hls.xml``,
    ``sr_hdf.xml``) in which ``sr_aerosol_qa`` is already renamed to ``CLOUD``.
    """
    try:
        root = etree.parse(str(path)).getroot()
    except etree.XMLSyntaxError:
        return False
    if root.tag != f"{{{ESPA_NS['e']}}}espa_metadata":
        return False
    return root.find(".//e:band[@name='sr_aerosol_qa']", ESPA_NS) is not None


def find_espa_xml(directory: Path) -> Path:
    """Find the single post-LaSRC ESPA metadata XML under a kept working dir."""
    candidates = [p for p in sorted(directory.rglob("*.xml")) if is_lasrc_espa_xml(p)]
    if len(candidates) != 1:
        raise click.ClickException(
            f"Expected exactly one ESPA XML under {directory}, found {candidates}"
        )
    return candidates[0]


def band_files(xml: Path) -> dict[str, Path]:
    """Map ESPA band name -> file path, relative to the XML's directory."""
    tree = etree.parse(str(xml))
    out = {}
    for band in tree.iterfind(".//e:band", ESPA_NS):
        name = band.get("name")
        file_name = band.findtext("e:file_name", namespaces=ESPA_NS)
        if name and file_name:
            out[name] = xml.parent / file_name
    return out


def read_window(path: Path, row: int, col: int, dtype: str) -> np.ndarray:
    with rasterio.open(path) as src:
        data = src.read(1, window=Window(col, row, SIZE, SIZE), boundless=False)
    if data.shape != (SIZE, SIZE):
        raise click.ClickException(
            f"Window at row={row} col={col} runs off {path} (got {data.shape})"
        )
    return data.astype(dtype)


def read_bands(xml: Path, spec: dict, row: int, col: int) -> dict[str, np.ndarray]:
    files = band_files(xml)
    out = {}
    for key, (names, dtype) in spec.items():
        name = next((n for n in names if n in files), None)
        if name is None:
            raise click.ClickException(
                f"None of bands {names} in {xml}; have {sorted(files)}"
            )
        out[key] = read_window(files[name], row, col, dtype)
    return out


@click.command()
@click.option(
    "--c-landsat-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--c-sentinel-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Kept C Sentinel-2 working dir; omit to skip the Sentinel-2 XML.",
)
@click.option(
    "--rust-landsat-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--row", type=int, required=True, help="Window row offset; pick a scene edge."
)
@click.option(
    "--col", type=int, required=True, help="Window column offset; pick a scene edge."
)
@click.option(
    "--out",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
)
def main(c_landsat_dir, c_sentinel_dir, rust_landsat_dir, row, col, out):
    """Cut LaSRC C/Rust reference fixtures."""
    out.mkdir(parents=True, exist_ok=True)

    ls_xml = find_espa_xml(c_landsat_dir)
    shutil.copyfile(ls_xml, out / "landsat_c_lasrc.xml")
    if c_sentinel_dir is not None:
        shutil.copyfile(find_espa_xml(c_sentinel_dir), out / "sentinel_c_lasrc.xml")

    arrays = read_bands(ls_xml, LANDSAT_INPUTS, row, col)
    arrays.update(read_bands(ls_xml, LANDSAT_C_OUTPUTS, row, col))
    fill = (arrays["qa_pixel"] & 1) != 0
    if not fill.any() or fill.all():
        click.echo(
            "WARNING: window has no mix of fill and valid pixels "
            f"(qa_pixel bit 0 fraction: {fill.mean():.3f}); adjust --row/--col",
            err=True,
        )
    np.savez_compressed(out / "landsat_toa_bt_window.npz", **arrays)

    rust_imgs = sorted(rust_landsat_dir.rglob("*_sr_band4.img"))
    if len(rust_imgs) != 1:
        raise click.ClickException(
            f"Expected one *_sr_band4.img under {rust_landsat_dir}, found {rust_imgs}"
        )
    sr = read_window(rust_imgs[0], row, col, "uint16")
    np.savez_compressed(out / "landsat_rust_sr_window.npz", sr_band4=sr)

    for p in sorted(out.iterdir()):
        click.echo(f"{p} ({p.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
