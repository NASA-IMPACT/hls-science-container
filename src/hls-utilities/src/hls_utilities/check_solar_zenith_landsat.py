from pathlib import Path

from hls_utilities.mtlutils import parsemeta

MAX_SOLAR_ZENITH = 76.0


def solar_zenith_is_valid(mtl: Path) -> bool:
    """Check the Landsat scene center solar zenith angle is within limits

    Parameters
    ----------
    mtl
        Path to the granule's `_MTL.txt` metadata file.
    """
    metadata = parsemeta(str(mtl))
    try:
        sun_elevation = float(
            metadata["L1_METADATA_FILE"]["IMAGE_ATTRIBUTES"]["SUN_ELEVATION"]
        )
    except KeyError:
        sun_elevation = float(
            metadata["LANDSAT_METADATA_FILE"]["IMAGE_ATTRIBUTES"]["SUN_ELEVATION"]
        )

    solar_zenith = 90 - sun_elevation
    return solar_zenith <= MAX_SOLAR_ZENITH
