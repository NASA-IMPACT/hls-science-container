"""Rust LaSRC tasks for the Sentinel-2 pipeline.

``RunLaSRCRust``, ``PrepareEspaInputAfterRust`` and ``RegisterLaSRC`` replace
C's ``PrepareEspaInput`` -> ``RunLaSRC`` and leave the granule directory as C
LaSRC would, so downstream tasks are unchanged.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass

from hls_utilities.register_lasrc_bands import (
    AERO_BAND,
    AERO_QA_BAND,
    SENTINEL_SR_BANDS,
    read_product_id,
    register_lasrc_bands,
)
from hls_workflows.base import AssetBundle, MappedTask, TaskFailure
from hls_workflows.common.lasrc_aux import (
    require_viirs_era,
    resolve_lasrc_aux_paths,
)
from hls_workflows.common.lasrc_version import espa_app_version
from hls_workflows.granules import Sentinel2Granule

from .assets import (
    CONFIG,
    EnvConfig,
    espa_xml_asset,
    fmask_bin_asset,
    lasrc_aerosol_qa_asset,
    quality_mask_applied_asset,
    rust_lasrc_dir_asset,
    safe_dir_asset,
)
from .mapped_tasks import PrepareEspaInput

logger = logging.getLogger(__name__)

_AERO_QA_SUFFIX = f"_{AERO_QA_BAND}.img"
_FILE_SUFFIXES = (".img", ".hdr", ".img.aux.xml")


@dataclass(frozen=True, kw_only=True)
class RunLaSRCRust(MappedTask):
    """Run the Rust LaSRC on the quality-masked SAFE scene, writing ESPA output.

    Requires Fmask so the SAFE is untouched until Fmask is done.
    """

    instrument = True
    requires_factory = lambda gid: (
        CONFIG,
        safe_dir_asset(gid),
        quality_mask_applied_asset(gid),
        fmask_bin_asset(gid),
    )
    provides_factory = lambda gid: (rust_lasrc_dir_asset(gid),)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        from lasrc.pipeline_sentinel import process_sentinel_scene

        config: EnvConfig = bundle[CONFIG]
        safe_dir = bundle[safe_dir_asset(self.granule_id)]

        granule = Sentinel2Granule.from_str(self.granule_id)
        require_viirs_era(granule.acquisition_time)
        aux = resolve_lasrc_aux_paths(
            is_sentinel=True, acquisition=granule.acquisition_time
        )

        output_dir = config.working_dir / self.granule_id / "lasrc_rs"
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Run LaSRC (rust)")
        # mission "S2B" -> sensor "SENTINEL_2B"
        process_sentinel_scene(
            safe_dir=safe_dir,
            angle_hdf=aux["angle_hdf"],
            intref_hdf=aux["intref_hdf"],
            transm_hdf=aux["transm_hdf"],
            sphera_hdf=aux["sphera_hdf"],
            viirs_aux_path=aux["wv_oz_hdf"],
            dem_path=aux["dem_hdf"],
            ratio_path=aux["ratio_hdf"],
            output_path=output_dir,
            sensor_name=f"SENTINEL_2{granule.mission[-1]}",
            output_format="espa",
        )

        return {rust_lasrc_dir_asset(self.granule_id): output_dir}


@dataclass(frozen=True, kw_only=True)
class PrepareEspaInputAfterRust(PrepareEspaInput):
    """PrepareEspaInput ordered after the Rust LaSRC, which reads the raw SAFE."""

    requires_factory = lambda gid: (
        *PrepareEspaInput.requires_factory(gid),  # type: ignore[misc]
        rust_lasrc_dir_asset(gid),
    )


@dataclass(frozen=True, kw_only=True)
class RegisterLaSRC(MappedTask):
    """Make Rust LaSRC output look like C LaSRC output.

    Moves the Rust outputs next to the ESPA XML under the ESPA product ID and
    registers every LaSRC band in the XML.
    """

    requires_factory = lambda gid: (
        CONFIG,
        espa_xml_asset(gid),
        rust_lasrc_dir_asset(gid),
    )
    provides_factory = lambda gid: (lasrc_aerosol_qa_asset(gid),)

    def run(self, bundle: AssetBundle) -> AssetBundle:
        espa_xml = bundle[espa_xml_asset(self.granule_id)]
        rust_dir = bundle[rust_lasrc_dir_asset(self.granule_id)]
        out_dir = espa_xml.parent

        product_id = read_product_id(espa_xml)

        qa_files = list(rust_dir.glob(f"*{_AERO_QA_SUFFIX}"))
        if len(qa_files) != 1:
            raise TaskFailure(
                f"Expected one '*{_AERO_QA_SUFFIX}' in {rust_dir}, found {qa_files}"
            )
        prefix = qa_files[0].name.removesuffix(_AERO_QA_SUFFIX)

        names = (*SENTINEL_SR_BANDS, AERO_BAND, AERO_QA_BAND)
        for name in names:
            for suffix in _FILE_SUFFIXES:
                src = rust_dir / f"{prefix}_{name}{suffix}"
                if src.exists():
                    shutil.move(src, out_dir / f"{product_id}_{name}{suffix}")

        granule = Sentinel2Granule.from_str(self.granule_id)
        aux = resolve_lasrc_aux_paths(
            is_sentinel=True, acquisition=granule.acquisition_time
        )

        logger.info("Register LaSRC (rust) bands")
        try:
            register_lasrc_bands(
                espa_xml,
                SENTINEL_SR_BANDS,
                app_version=espa_app_version(),
                aux_source=aux["wv_oz_hdf"].name,
            )
        except FileNotFoundError as exc:
            raise TaskFailure(str(exc)) from exc

        return {
            lasrc_aerosol_qa_asset(self.granule_id): out_dir
            / f"{product_id}{_AERO_QA_SUFFIX}"
        }
