"""Collection 2 output encoding constants and conversions used by LaSRC.

Constants and the encoding arithmetic follow ``output.h`` and ``convert_output``
in espa-surface-reflectance v3.5.1.0.
"""

import numpy as np

SCALE_FACTOR_REFL = 0.0000275
OFFSET_REFL = -0.2
SCALE_FACTOR_TH = 0.00341802
OFFSET_TH = 149.0
SCALE_FACTOR_AERO = 0.001
FILL_VALUE = 0
FILL_VALUE_AERO = -9999

MIN_VALID_REFL = -0.2
MAX_VALID_REFL = 1.6
MIN_VALID_TH = 150.0
MAX_VALID_TH = 350.0
MIN_VALID_AERO = 0.0
MAX_VALID_AERO = 5.0

_USHRT_MAX = 65535.0


def _encode(
    values: np.ndarray,
    scale: float,
    offset: float,
    min_valid: float,
    max_valid: float,
) -> np.ndarray:
    # C holds the offset and multiplier as float and the clamp bounds as double
    offset_value = np.float32(-offset)
    mult_value = np.float32(1.0 / scale)
    min_value = max((min_valid + float(offset_value)) * float(mult_value), 0.0)
    max_value = min((max_valid + float(offset_value)) * float(mult_value), _USHRT_MAX)

    values = np.asarray(values, dtype=np.float32)
    tmp = (values + offset_value) * mult_value
    # roundf rounds half away from zero; tmp is non-negative past the clamp.
    # Add 0.5 in float64 so float32 rounding cannot push x.49999 up to x+1.
    tmp64 = tmp.astype(np.float64)
    rounded = np.floor(tmp64 + 0.5)
    # C promotes the float to double for the range checks
    out = np.where(
        tmp64 < min_value,
        np.trunc(min_value),
        np.where(tmp64 > max_value, np.trunc(max_value), rounded),
    )
    out = np.where(values == FILL_VALUE, FILL_VALUE, out)
    return out.astype(np.uint16)


def encode_reflectance(values: np.ndarray) -> np.ndarray:
    """Encode unscaled reflectance to uint16 DN; 0.0 becomes fill (0)."""
    return _encode(
        values, SCALE_FACTOR_REFL, OFFSET_REFL, MIN_VALID_REFL, MAX_VALID_REFL
    )


def encode_thermal(values: np.ndarray) -> np.ndarray:
    """Encode brightness temperature (K) to uint16 DN; 0.0 becomes fill (0)."""
    return _encode(values, SCALE_FACTOR_TH, OFFSET_TH, MIN_VALID_TH, MAX_VALID_TH)


def decode_reflectance(dn: np.ndarray) -> np.ndarray:
    """Decode uint16 reflectance DN to float32, with NaN at fill."""
    dn = np.asarray(dn)
    refl = (dn.astype(np.float64) * SCALE_FACTOR_REFL + OFFSET_REFL).astype(np.float32)
    refl[dn == FILL_VALUE] = np.nan
    return refl
