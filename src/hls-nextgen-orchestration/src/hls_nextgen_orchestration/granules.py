from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


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
class HlsGranule:
    """
    Represents an HLS v2 Granule ID.

    Attributes
    ----------
    product : str
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

    product: str
    sensor: str
    tile_id: str
    acquisition_time: dt.datetime
    version_major: str = "v2"
    version_minor: str = "0"

    def __post_init__(self) -> None:
        """Validate granule attributes"""
        if self.tile_id.startswith("T"):
            raise ValueError(
                f"tile_id must be the raw MGRS code (starting with a digit). Found prefix 'T' in: {self.tile_id}"
            )

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

        # Extract MGRS tile, stripping the 'T' prefix if present
        raw_tile_id = parts[2].lstrip("T")

        # Timestamp format: YYYYDDDTHHMMSS (Year + Day of Year + Time)
        acq_time = dt.datetime.strptime(parts[3], "%Y%jT%H%M%S")

        return cls(
            product=parts[0],
            sensor=parts[1],
            tile_id=raw_tile_id,
            acquisition_time=acq_time,
            version_major=parts[4],
            version_minor=parts[5],
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
                self.version_major,
                self.version_minor,
            ]
        )
