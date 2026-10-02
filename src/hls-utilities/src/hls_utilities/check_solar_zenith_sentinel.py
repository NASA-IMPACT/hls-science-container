from pathlib import Path

from lxml import etree

MAX_SOLAR_ZENITH = 76.0


def solar_zenith_is_valid(mtd_tl: Path) -> bool:
    """Check the Sentinel-2 mean solar zenith angle is within limits

    Parameters
    ----------
    mtd_tl
        Path to the granule's `MTD_TL.xml` metadata file.
    """
    text = etree.parse(mtd_tl).findtext(".//Mean_Sun_Angle/ZENITH_ANGLE")
    if text is None:
        raise ValueError(f"No Mean_Sun_Angle/ZENITH_ANGLE found in {mtd_tl}")
    solar_zenith = float(text)
    return solar_zenith <= MAX_SOLAR_ZENITH
