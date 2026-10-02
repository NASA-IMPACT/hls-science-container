from pathlib import Path

from espa import Metadata

HLS_PRODUCT = "hls"

BAND_RENAMES: dict[str, str] = {
    "sr_band1": "band01",
    "sr_band2": "band02-blue",
    "sr_band3": "band03-green",
    "sr_band4": "band04-red",
    "sr_band5": "band05",
    "sr_band6": "band06",
    "sr_band7": "band07",
    "radsat_qa": "bandQA",
    "toa_band9": "band09",
    "bt_band10": "band10",
    "bt_band11": "band11",
    "sr_aerosol_qa": "CLOUD",
}


def create_landsat_sr_hdf_xml(input_xml: Path | str, output_xml: Path | str) -> None:
    """Create the ESPA XML for the Landsat HLS SR HDF

    HLS bands are renamed to their HLS names, and all other bands are removed.
    """
    mm = Metadata(xml_filename=str(input_xml))
    mm.parse()
    for band in mm.xml_object.bands.iterchildren():
        name = band.get("name")
        if name in BAND_RENAMES:
            band.set("name", BAND_RENAMES[name])
            band.set("product", HLS_PRODUCT)

    for band in mm.xml_object.bands.iterchildren():
        if band.get("product") != HLS_PRODUCT:
            mm.xml_object.bands.remove(band)

    mm.write(xml_filename=str(output_xml))
