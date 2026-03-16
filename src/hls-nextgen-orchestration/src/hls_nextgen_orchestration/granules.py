from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import cast, get_args

from .constants import HLS_VERSION, PRODUCTS, HlsVersion


@dataclass
class LandsatGranule:
    """
    Represents a USGS Landsat Collection 2 Granule ID.

    Example ID: LC08_L1TP_032034_20200908_20200918_02_T1

    Attributes
    ----------
    platform : str
        The platform identifier (e.g., 'LC08').
    processing_level : str
        The processing level (e.g., 'L1TP').
    path : int
        The WRS-2 path (e.g., 32).
    row : int
        The WRS-2 row (e.g., 34).
    acquisition_date : dt.datetime
        The acquisition date.
    processing_date : dt.datetime
        The processing date.
    collection_number : str
        The collection number (e.g., '02').
    collection_tier : str
        The collection tier (e.g., 'T1').
    """

    platform: str
    processing_level: str
    path: int
    row: int
    acquisition_date: dt.datetime
    processing_date: dt.datetime
    collection_number: str
    collection_tier: str

    @classmethod
    def from_str(cls, granule_id: str) -> LandsatGranule:
        """
        Parse a Landsat Collection 2 granule ID string.
        """
        parts = granule_id.split("_")
        if len(parts) != 7:
            raise ValueError(f"Invalid Landsat Collection 2 ID format: {granule_id}")

        # Path and Row are combined in the ID (e.g., 032034)
        path_row = parts[2]
        path = int(path_row[:3])
        row = int(path_row[3:])

        return cls(
            platform=parts[0],
            processing_level=parts[1],
            path=path,
            row=row,
            acquisition_date=dt.datetime.strptime(parts[3], "%Y%m%d"),
            processing_date=dt.datetime.strptime(parts[4], "%Y%m%d"),
            collection_number=parts[5],
            collection_tier=parts[6],
        )

    def to_str(self) -> str:
        """
        Reconstruct the Landsat granule ID string.
        """
        return "_".join(
            [
                self.platform,
                self.processing_level,
                self.path_row,
                self.acquisition_date.strftime("%Y%m%d"),
                self.processing_date.strftime("%Y%m%d"),
                self.collection_number,
                self.collection_tier,
            ]
        )

    @property
    def path_row(self) -> str:
        """WRS-2 path-row string"""
        return f"{self.path:03d}{self.row:03d}"


@dataclass
class Sentinel2Granule:
    """Represents a Sentinel-2 L1C Granule ID.

    Examples
    --------
    S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841

    Notes
    -----
    The SAFE (Standard Archive Format for Europe) product format layout
    looks like (abbre)

    {PRODUCT_ID}/
        DATASTRIP/*
        GRANULE/
            {GRANULE_ID}/
                AUX_DATA/
                IMG_DATA/
                QI_DATA/
                MTD_TL.xml
        HTML/*
        INSPIRE.xml
        manifest.safe
        MTD_MSIL1C.xml

    By convention within the HLS project we call the "product ID" (outer ID) the
    "granule ID" and ignore the "inner" granule identifier. When this format
    was originally released there would be multiple granules within each product,
    but now there's only 1 granule within the product so they are one-to-one.

    Attributes
    ----------
    mission : str
        The mission identifier (e.g., 'S2A', 'S2B').
    product_level : str
        The product level (e.g., 'MSIL1C').
    acquisition_time : dt.datetime
        The sensing start time.
    processing_baseline : str
        The processing baseline (e.g., 'N0208').
    relative_orbit : str
        The relative orbit number (e.g., 'R065').
    tile_id : str
        The MGRS tile identifier (e.g., '32TQM').
    product_time : dt.datetime
        The product generation time.
    """

    mission: str
    product_level: str
    acquisition_time: dt.datetime
    processing_baseline: str
    relative_orbit: str
    tile_id: str
    product_time: dt.datetime

    @classmethod
    def from_str(cls, granule_id: str) -> Sentinel2Granule:
        """Parse a Sentinel-2 L1C SAFE ID string."""
        parts = granule_id.split("_")
        if len(parts) != 7:
            raise ValueError(f"Invalid Sentinel-2 ID format: {granule_id}")

        return cls(
            mission=parts[0],
            product_level=parts[1],
            acquisition_time=dt.datetime.strptime(parts[2], "%Y%m%dT%H%M%S"),
            processing_baseline=parts[3],
            relative_orbit=parts[4],
            tile_id=parts[5].lstrip("T"),
            product_time=dt.datetime.strptime(parts[6], "%Y%m%dT%H%M%S"),
        )

    def to_str(self) -> str:
        """
        Reconstruct the Sentinel-2 SAFE ID string.

        Returns
        -------
        str
            The formatted Sentinel-2 SAFE ID.
        """
        return "_".join(
            [
                self.mission,
                self.product_level,
                self.acquisition_time.strftime("%Y%m%dT%H%M%S"),
                self.processing_baseline,
                self.relative_orbit,
                f"T{self.tile_id}",
                self.product_time.strftime("%Y%m%dT%H%M%S"),
            ]
        )


@dataclass
class HlsGranule:
    """Represents an HLS v2 Granule ID.

    Example: HLS.S30.T18TYL.2020001T153621.v2.0

    Attributes
    ----------
    product : Literal
        The product name (e.g., 'HLS').
    sensor : str
        The sensor identifier (e.g., 'S30', 'L30').
    tile_id : str
        The MGRS tile identifier without the leading 'T' (e.g., '18TYL').
    acquisition_time : dt.datetime
        The acquisition timestamp.
    version_major : str
        The major version string (e.g., 'v2').
    version_minor : str
        The minor version string (e.g., '0').
    """

    product: PRODUCTS
    sensor: str
    tile_id: str
    acquisition_time: dt.datetime
    version: HlsVersion = HLS_VERSION

    def __post_init__(self) -> None:
        """Validate granule attributes"""
        if self.tile_id.startswith("T"):
            raise ValueError(
                f"tile_id must be the raw MGRS code (starting with a digit). Found prefix 'T' in: {self.tile_id}"
            )

    @classmethod
    def from_sentinel2(cls, product: PRODUCTS, granule: Sentinel2Granule) -> HlsGranule:
        """Convert from a Sentinel-2 granule ID for a HLS product"""
        hls_granule = HlsGranule(
            product=product,
            sensor="S30",
            tile_id=granule.tile_id,
            acquisition_time=granule.acquisition_time,
        )
        return hls_granule

    @classmethod
    def from_str(cls, granule_id: str) -> HlsGranule:
        """
        Parse an HLS v2 granule ID string.

        Parameters
        ----------
        granule_id : str
            The HLS ID (e.g., 'HLS.S30.T18TYL.2020001T153621.v2.0').

        Returns
        -------
        HlsGranule
            The parsed HLS granule object.

        Raises
        ------
        ValueError
            If the granule_id format is incorrect.
        """
        parts = granule_id.split(".")
        if len(parts) != 6:
            raise ValueError(f"Invalid HLS v2 ID format: {granule_id}")

        product = parts[0]
        products = get_args(PRODUCTS)
        if product not in products:
            raise ValueError(f"Unknown product {parts[0]} (expected {products})")
        product = cast(PRODUCTS, parts[0])

        # Extract MGRS tile, stripping the 'T' prefix if present
        raw_tile_id = parts[2].lstrip("T")

        # Timestamp format: YYYYDDDTHHMMSS (Year + Day of Year + Time)
        acq_time = dt.datetime.strptime(parts[3], "%Y%jT%H%M%S")

        return cls(
            product=product,
            sensor=parts[1],
            tile_id=raw_tile_id,
            acquisition_time=acq_time,
            version=HlsVersion.from_str(f"{parts[4]}.{parts[5]}"),
        )

    def to_str(self) -> str:
        """
        Reconstruct the HLS granule ID string.

        Returns
        -------
        str
            The formatted granule ID.
        """
        return ".".join(
            [
                self.product,
                self.sensor,
                f"T{self.tile_id}",
                self.acquisition_time.strftime("%Y%jT%H%M%S"),
                self.version.to_str(),
            ]
        )
