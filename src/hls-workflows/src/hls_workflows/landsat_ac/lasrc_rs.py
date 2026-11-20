"""Rust LaSRC tasks for the Landsat AC pipeline.

``RunLaSRCRust`` followed by ``RegisterLaSRC`` replace C's ``RunLaSRC`` and
leave the granule directory as C LaSRC would, so downstream tasks are unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from hls_utilities.landsat_toa_bt import write_landsat_toa_bt
from hls_utilities.register_lasrc_bands import (
    LANDSAT_SR_BANDS,
    read_product_id,
    register_lasrc_bands,
)
from hls_workflows.base import Asset, AssetBundle, Task, TaskFailure
from hls_workflows.common.lasrc_aux import (
    require_viirs_era,
    resolve_lasrc_aux_paths,
)
from hls_workflows.common.lasrc_version import espa_app_version

from .assets import (
    CONFIG,
    ESPA_XML,
    LASRC_DONE,
    RUST_LASRC_DONE,
    SOLAR_VALID,
    EnvConfig,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunLaSRCRust(Task):
    """Run the Rust LaSRC on the raw Landsat scene, writing ESPA output."""

    instrument = True
    requires = (CONFIG, ESPA_XML, SOLAR_VALID)
    provides = (RUST_LASRC_DONE,)

    def run(self, inputs: AssetBundle) -> dict[Asset[bool], bool]:
        from lasrc.pipeline import AuxFilePaths, process_scene

        config: EnvConfig = inputs[CONFIG]
        granule_dir = inputs[ESPA_XML].parent

        granule = config.landsat_granule
        require_viirs_era(granule.acquisition_date)
        aux_files = AuxFilePaths(
            **resolve_lasrc_aux_paths(
                is_sentinel=False, acquisition=granule.acquisition_date
            )
        )

        logger.info("Run LaSRC (rust)")
        # platform "LC09" -> sensor "LANDSAT_9"
        process_scene(
            input_path=granule_dir,
            aux_files=aux_files,
            output_path=granule_dir,
            sensor_name=f"LANDSAT_{granule.platform[-1]}",
            output_format="espa",
        )

        return {RUST_LASRC_DONE: True}


@dataclass(frozen=True)
class RegisterLaSRC(Task):
    """Make Rust LaSRC output look like C LaSRC output.

    Drops the unused ``sr_band9``, writes the TOA/BT bands that C LaSRC
    produces, and registers every LaSRC band in the ESPA XML.
    """

    instrument = True
    requires = (CONFIG, ESPA_XML, RUST_LASRC_DONE)
    provides = (LASRC_DONE,)

    def run(self, inputs: AssetBundle) -> dict[Asset[bool], bool]:
        config: EnvConfig = inputs[CONFIG]
        espa_xml: Path = inputs[ESPA_XML]
        granule_dir = espa_xml.parent
        product_id = read_product_id(espa_xml)

        for suffix in (".img", ".hdr", ".img.aux.xml"):
            (granule_dir / f"{product_id}_sr_band9{suffix}").unlink(missing_ok=True)

        aux_files = resolve_lasrc_aux_paths(
            is_sentinel=False, acquisition=config.landsat_granule.acquisition_date
        )

        logger.info("Register LaSRC (rust) bands")
        written = write_landsat_toa_bt(espa_xml)
        bands = (*written, *LANDSAT_SR_BANDS)
        try:
            register_lasrc_bands(
                espa_xml,
                bands,
                app_version=espa_app_version(),
                aux_source=aux_files["wv_oz_hdf"].name,
            )
        except FileNotFoundError as exc:
            raise TaskFailure(str(exc)) from exc

        return {LASRC_DONE: True}
