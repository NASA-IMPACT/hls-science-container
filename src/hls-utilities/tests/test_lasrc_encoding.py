from pathlib import Path

import numpy as np

from hls_utilities.lasrc_encoding import (
    MAX_VALID_REFL,
    MIN_VALID_REFL,
    decode_reflectance,
)

DATA = Path(__file__).parent / "data" / "lasrc"


def test_rust_sr_decodes_into_valid_range() -> None:
    dn = np.load(DATA / "landsat_rust_sr_window.npz")["sr_band4"]
    refl = decode_reflectance(dn)
    valid = refl[~np.isnan(refl)]
    assert valid.size > 0
    assert valid.min() >= MIN_VALID_REFL and valid.max() <= MAX_VALID_REFL
