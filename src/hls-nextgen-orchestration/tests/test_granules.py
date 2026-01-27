import datetime as dt

import pytest

from hls_nextgen_orchestration.granules import HlsGranule, LandsatGranule


# --- LandsatGranule Tests ---
class TestLandsatGranule:
    def test_from_str_valid_lc08(self) -> None:
        """
        Test parsing a standard Landsat 8 Collection 2 ID.

        Verifies that all fields are extracted correctly from a
        canonical LC08 ID string.
        """
        granule_id = "LC08_L1TP_046028_20200908_20200918_02_T1"
        granule = LandsatGranule.from_str(granule_id)

        assert granule.platform == "LC08"
        assert granule.processing_level == "L1TP"
        assert granule.path == 46
        assert granule.row == 28
        assert granule.acquisition_date == dt.datetime(2020, 9, 8)
        assert granule.processing_date == dt.datetime(2020, 9, 18)
        assert granule.collection_number == "02"
        assert granule.collection_category == "T1"
        assert granule.sensor == "LC08"

    def test_from_str_valid_le07(self) -> None:
        """
        Test parsing a Landsat 7 ID.

        Verifies path and row extraction for LE07 platform.
        """
        granule_id = "LE07_L1TP_012031_20150101_20150201_02_T1"
        granule = LandsatGranule.from_str(granule_id)

        assert granule.platform == "LE07"
        assert granule.path == 12
        assert granule.row == 31

    def test_path_row_padding(self) -> None:
        """
        Test that path_row property correctly pads integers.

        Verifies that single digit path/rows become 3-digit zero-padded strings.
        """
        granule = LandsatGranule(
            platform="LC08",
            processing_level="L1TP",
            path=5,  # Should become '005'
            row=10,  # Should become '010'
            acquisition_date=dt.datetime(2022, 1, 1),
            processing_date=dt.datetime(2022, 1, 5),
            collection_number="02",
            collection_category="T1",
        )
        assert granule.path_row == "005010"

    def test_to_str_roundtrip(self) -> None:
        """
        Test that to_str recreates the exact original ID.

        Verifies that parsing a string and converting it back results in
        identity.
        """
        original_id = "LC08_L1TP_046028_20200908_20200918_02_T1"
        granule = LandsatGranule.from_str(original_id)
        assert granule.to_str() == original_id

    def test_invalid_format_raises_error(self) -> None:
        """
        Test that malformed IDs raise ValueError.

        Verifies that IDs missing components raise the appropriate exception.
        """
        # Missing one component (only 6 parts)
        invalid_id = "LC08_L1TP_046028_20200908_20200918_02"
        with pytest.raises(ValueError) as excinfo:
            LandsatGranule.from_str(invalid_id)
        assert "Invalid Landsat Collection 2 ID format" in str(excinfo.value)

    def test_invalid_date_raises_error(self) -> None:
        """
        Test that invalid date strings raise standard parsing errors.

        Verifies that impossible dates (e.g. Month 13) fail parsing.
        """
        # Month 13 is invalid
        invalid_id = "LC08_L1TP_046028_20201308_20200918_02_T1"
        with pytest.raises(ValueError):
            LandsatGranule.from_str(invalid_id)


# --- HlsGranule Tests ---
class TestHlsGranule:
    def test_from_str_valid_s30(self) -> None:
        """
        Test parsing a standard HLS S30 (Sentinel) ID.

        Verifies correct extraction of tile ID and timestamp from S30 format.
        """
        # Format: HLS.S30.T18TYL.2020001T153621.v2.0
        # 2020001 = Jan 1, 2020
        granule_id = "HLS.S30.T18TYL.2020001T153621.v2.0"
        granule = HlsGranule.from_str(granule_id)

        assert granule.product == "HLS"
        assert granule.sensor == "S30"
        assert granule.tile_id == "T18TYL"
        # 2020 day 1 at 15:36:21
        assert granule.acquisition_time == dt.datetime(2020, 1, 1, 15, 36, 21)
        assert granule.version_major == "v2"
        assert granule.version_minor == "0"
        assert granule.mgrs_grid == "T18TYL"

    def test_from_str_valid_l30(self) -> None:
        """
        Test parsing a standard HLS L30 (Landsat) ID.

        Verifies correct extraction of year and hour from L30 format.
        """
        # 2021252 = Sept 9, 2021 (non-leap year check logic handled by datetime)
        granule_id = "HLS.L30.T10SEG.2021252T185631.v2.0"
        granule = HlsGranule.from_str(granule_id)

        assert granule.sensor == "L30"
        assert granule.acquisition_time.year == 2021
        assert granule.acquisition_time.hour == 18

    def test_to_str_roundtrip(self) -> None:
        """
        Test that to_str recreates the exact original ID.

        Verifies that parsing a string and converting it back results in
        identity.
        """
        original_id = "HLS.S30.T18TYL.2020001T153621.v2.0"
        granule = HlsGranule.from_str(original_id)
        assert granule.to_str() == original_id

    def test_invalid_format_raises_error(self) -> None:
        """
        Test that malformed IDs raise ValueError.

        Verifies that IDs missing version components raise the appropriate exception.
        """
        # Missing version minor
        invalid_id = "HLS.S30.T18TYL.2020001T153621.v2"
        with pytest.raises(ValueError) as excinfo:
            HlsGranule.from_str(invalid_id)
        assert "Invalid HLS v2 ID format" in str(excinfo.value)

    def test_invalid_julian_date(self) -> None:
        """
        Test invalid julian date parsing.

        Verifies that impossible julian days (e.g. 400) fail parsing.
        """
        # Day 400 is invalid
        invalid_id = "HLS.S30.T18TYL.2020400T153621.v2.0"
        with pytest.raises(ValueError):
            HlsGranule.from_str(invalid_id)
