import logging
from pathlib import Path
from typing import Literal

from espa import Metadata

logger = logging.getLogger(__name__)

HLS_PRODUCT = "hls"

# Sentinel-2 bands are split across two HDF files ("parts"), with the ESPA band
# names renamed to their HLS equivalents
BAND_RENAMES: dict[str, dict[str, str]] = {
    "one": {
        "sr_band1": "band01",
        "sr_band2": "blue",
        "sr_band3": "green",
        "sr_band4": "red",
        "sr_band5": "band05",
        "sr_band6": "band06",
        "sr_band7": "band07",
        "sr_band8": "band08",
    },
    "two": {
        "sr_band8a": "band8a",
        "sr_band9": "band09",
        "sr_band10": "band10",
        "sr_band11": "band11",
        "sr_band12": "band12",
        "sr_aerosol_qa": "CLOUD",
    },
}


def create_sr_hdf_xml(
    input_xml: Path | str,
    output_xml: Path | str,
    part: Literal["one", "two"],
) -> None:
    """Create the ESPA XML for one part of the Sentinel-2 HLS SR HDF

    Bands belonging to the requested part are renamed to their HLS names, and
    all other bands are removed.
    """
    renames = BAND_RENAMES[part]

    mm = Metadata(xml_filename=str(input_xml))
    mm.parse()
    for band in mm.xml_object.bands.iterchildren():
        name = band.get("name")
        if name in renames:
            band.set("name", renames[name])
            band.set("product", HLS_PRODUCT)

    for band in mm.xml_object.bands.iterchildren():
        if band.get("product") != HLS_PRODUCT:
            logger.debug(f"Removing band {band.get('name')}")
            mm.xml_object.bands.remove(band)

    mm.write(xml_filename=str(output_xml))
