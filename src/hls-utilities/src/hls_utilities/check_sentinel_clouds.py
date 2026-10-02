from pathlib import Path

from lxml import etree

MAX_CLOUD_COVER = 95.0


def cloud_cover_is_valid(mtd_msil1c: Path) -> bool:
    """Check the Sentinel-2 L1C cloud cover assessment is within limits

    Parameters
    ----------
    mtd_msil1c
        Path to the granule's `MTD_MSIL1C.xml` metadata file.
    """
    text = etree.parse(mtd_msil1c).findtext(".//Cloud_Coverage_Assessment")
    if text is None:
        raise ValueError(f"No Cloud_Coverage_Assessment found in {mtd_msil1c}")
    cloud = float(text)
    return cloud <= MAX_CLOUD_COVER
