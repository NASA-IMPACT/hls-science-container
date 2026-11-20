"""HLS Landsat TOA band 9 and brightness temperature outputs.

C LaSRC writes ``toa_band9`` and ``bt_band10``/``bt_band11`` next to SR. The
math here reproduces ``compute_landsat_toa_refl`` (espa-surface-reflectance
v3.5.1.0) including its float/double mixing, so outputs match bit-for-bit.
"""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from espa import Metadata

from hls_utilities.lasrc_encoding import (
    MAX_VALID_REFL,
    MAX_VALID_TH,
    MIN_VALID_REFL,
    MIN_VALID_TH,
    encode_reflectance,
    encode_thermal,
)

DEG2RAD = 0.017453293
_F32 = np.float32

_FILL_BIT = 0


@dataclass(frozen=True)
class ToaBtParams:
    """Calibration parameters from the ESPA XML (thermal is None for OLI-only)."""

    refl_gain_b9: float
    refl_bias_b9: float
    sza_gain: float
    sza_bias: float
    rad_gain: tuple[float, float] | None
    rad_bias: tuple[float, float] | None
    k1: tuple[float, float] | None
    k2: tuple[float, float] | None


def _bands(mm: Metadata) -> dict[str, object]:
    return {str(b.get("name")): b for b in mm.xml_object.bands.iterchildren()}


def _f32(value: object) -> float:
    # C stores these XML values as float
    return float(_F32(float(str(value))))


def read_toa_bt_params(espa_xml: Path) -> ToaBtParams:
    """Read calibration constants from an ESPA XML the way LaSRC input.c does."""
    mm = Metadata(xml_filename=str(espa_xml))
    mm.parse()
    bands = _bands(mm)

    b9 = bands["b9"]
    sza = bands["sza"]
    sza_bias = sza.get("add_offset")  # type: ignore[attr-defined]

    thermal = str(mm.xml_object.global_metadata.instrument) != "OLI"
    rad_gain = rad_bias = k1 = k2 = None
    if thermal:
        t = [bands["b10"], bands["b11"]]
        rad_gain = tuple(_f32(b.radiance.get("gain")) for b in t)  # type: ignore[attr-defined]
        rad_bias = tuple(_f32(b.radiance.get("bias")) for b in t)  # type: ignore[attr-defined]
        k1 = tuple(_f32(b.thermal_const.get("k1")) for b in t)  # type: ignore[attr-defined]
        k2 = tuple(_f32(b.thermal_const.get("k2")) for b in t)  # type: ignore[attr-defined]

    return ToaBtParams(
        refl_gain_b9=_f32(b9.reflectance.get("gain")),  # type: ignore[attr-defined]
        refl_bias_b9=_f32(b9.reflectance.get("bias")),  # type: ignore[attr-defined]
        sza_gain=_f32(sza.get("scale_factor")),  # type: ignore[attr-defined]
        sza_bias=0.0 if sza_bias is None else _f32(sza_bias),
        rad_gain=rad_gain,  # type: ignore[arg-type]
        rad_bias=rad_bias,  # type: ignore[arg-type]
        k1=k1,  # type: ignore[arg-type]
        k2=k2,  # type: ignore[arg-type]
    )


def level1_qa_is_fill(qa_pixel: np.ndarray) -> np.ndarray:
    """True where the Level-1 QA designated fill bit is set."""
    fill: np.ndarray = ((np.asarray(qa_pixel).astype(np.uint16) >> _FILL_BIT) & 1) == 1
    return fill


def toa_reflectance(
    dn: np.ndarray,
    sza: np.ndarray,
    fill: np.ndarray,
    gain: float,
    bias: float,
    sza_gain: float,
    sza_bias: float,
) -> np.ndarray:
    """Unscaled float32 TOA reflectance corrected by per-pixel solar zenith."""
    ang = sza.astype(_F32) * _F32(sza_gain) + _F32(sza_bias)
    # cos() is evaluated in double and then stored to a float
    xmus = np.cos(ang.astype(np.float64) * DEG2RAD).astype(_F32)
    rotoa = dn.astype(_F32) * _F32(gain) + _F32(bias)
    rotoa = rotoa / xmus
    # C compares in double and stores the float limits
    rotoa64 = rotoa.astype(np.float64)
    out = np.where(
        rotoa64 < MIN_VALID_REFL,
        _F32(MIN_VALID_REFL),
        np.where(rotoa64 > MAX_VALID_REFL, _F32(MAX_VALID_REFL), rotoa),
    ).astype(_F32)
    out[fill] = 0.0
    return out


def brightness_temperature(
    dn: np.ndarray,
    fill: np.ndarray,
    gain: float,
    bias: float,
    k1: float,
    k2: float,
) -> np.ndarray:
    """Unscaled float32 brightness temperature in Kelvin."""
    with np.errstate(all="ignore"):
        rad = _F32(gain) * dn.astype(_F32) + _F32(bias)
        # k1 / rad is float, "+ 1.0" promotes to double, log and k2 / are double
        ratio = (_F32(k1) / rad).astype(np.float64) + 1.0
        bt = (float(_F32(k2)) / np.log(ratio)).astype(_F32)
    bt64 = bt.astype(np.float64)
    out = np.where(
        bt64 < MIN_VALID_TH,
        _F32(MIN_VALID_TH),
        np.where(bt64 > MAX_VALID_TH, _F32(MAX_VALID_TH), bt),
    ).astype(_F32)
    out[fill] = 0.0
    return out


def _band_path(espa_xml: Path, band: object) -> Path:
    return espa_xml.parent / str(band.file_name)  # type: ignore[attr-defined]


def _read(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        data: np.ndarray = src.read(1)
        return data


def write_landsat_toa_bt(espa_xml: Path) -> list[str]:
    """Write toa_band9 and (if thermal) bt_band10/bt_band11 next to the XML.

    Output is ENVI uint16 with nodata 0, georeferenced like the XML's b9 file.
    Returns the names of the bands written.
    """
    espa_xml = Path(espa_xml)
    params = read_toa_bt_params(espa_xml)
    mm = Metadata(xml_filename=str(espa_xml))
    mm.parse()
    bands = _bands(mm)
    product_id = str(mm.xml_object.global_metadata.product_id)

    fill = level1_qa_is_fill(_read(_band_path(espa_xml, bands["qa_pixel"])))

    b9_path = _band_path(espa_xml, bands["b9"])
    with rasterio.open(b9_path) as src:
        profile = {
            "driver": "ENVI",
            "dtype": "uint16",
            "count": 1,
            "nodata": 0,
            "width": src.width,
            "height": src.height,
            "crs": src.crs,
            "transform": src.transform,
        }
        b9 = src.read(1)

    outputs: dict[str, np.ndarray] = {}
    sza = _read(_band_path(espa_xml, bands["sza"]))
    toa = toa_reflectance(
        b9,
        sza,
        fill,
        params.refl_gain_b9,
        params.refl_bias_b9,
        params.sza_gain,
        params.sza_bias,
    )
    outputs["toa_band9"] = encode_reflectance(toa)

    if (
        params.rad_gain is not None
        and params.rad_bias is not None
        and params.k1 is not None
        and params.k2 is not None
    ):
        for i, name in enumerate(("b10", "b11")):
            dn = _read(_band_path(espa_xml, bands[name]))
            bt = brightness_temperature(
                dn,
                fill,
                params.rad_gain[i],
                params.rad_bias[i],
                params.k1[i],
                params.k2[i],
            )
            outputs[f"bt_band1{i}"] = encode_thermal(bt)

    for name, data in outputs.items():
        out_path = espa_xml.parent / f"{product_id}_{name}.img"
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(data, 1)
    return list(outputs)
