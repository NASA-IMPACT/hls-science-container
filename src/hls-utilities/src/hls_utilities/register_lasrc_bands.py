"""Register LaSRC output bands in an ESPA XML.

C LaSRC (espa-surface-reflectance v3.5.1.0) appends one ``<band>`` per output
to the ESPA XML after writing it. Rust LaSRC writes only the ``.img`` files, so
the same ``<band>`` entries are appended here, field for field.
"""

import os
import time
from collections.abc import Sequence
from pathlib import Path

from espa import Metadata
from lxml import etree

from hls_utilities import lasrc_encoding as enc

LANDSAT_SR_BANDS = tuple(f"sr_band{i}" for i in range(1, 8))
SENTINEL_SR_BANDS = (
    *(f"sr_band{i}" for i in range(1, 9)),
    "sr_band8a",
    *(f"sr_band{i}" for i in range(9, 13)),
)
LANDSAT_TOA_BT_BANDS = ("toa_band9", "bt_band10", "bt_band11")

AERO_BAND = "sr_aerosol"
AERO_QA_BAND = "sr_aerosol_qa"

_LANDSAT_AERO_QA_BITS = (
    "fill",
    "valid aerosol retrieval (center pixel of 3x3 window)",
    "water pixel (or water pixel was used in the fill-the-window interpolation)",
    "not used",
    "not used",
    "non-center window pixel for which aerosol was interpolated from "
    "surrounding 3x3 center pixels",
    "aerosol level",
    "aerosol level",
)
_SENTINEL_AERO_QA_BITS = (
    "fill",
    "valid aerosol retrieval (land and water)",
    "water",
    "failed aerosol retrieval",
    'aerosol retrieval value was interpolated or "fixed" from surrounding aerosols',
    "not used",
    "aerosol level",
    "aerosol level",
)


def _valid_range(
    min_valid: float, max_valid: float, scale: float, offset: float
) -> tuple[float, float]:
    # C computes (valid + band_offset) * mult_factor in double precision
    mult = 1.0 / scale
    return ((min_valid - offset) * mult, (max_valid - offset) * mult)


def _find_band(bands: etree._Element, name: str, product_prefix: str) -> etree._Element:
    for band in bands.iterchildren():
        if band.get("name") == name and str(band.get("product")).startswith(
            product_prefix
        ):
            return band
    raise ValueError(f"Band {name} ({product_prefix}*) not found in ESPA XML")


def _short_name(band: etree._Element, ns: str, n_keep: int) -> str:
    return str(band.findtext(f"{{{ns}}}short_name"))[:n_keep]


def read_product_id(espa_xml: Path) -> str:
    """Return ``global_metadata/product_id`` of an ESPA XML."""
    ns = {"e": "http://espa.cr.usgs.gov/v2"}
    return str(
        etree.parse(str(espa_xml)).xpath(
            "string(//e:global_metadata/e:product_id)", namespaces=ns
        )
    )


def register_lasrc_bands(
    espa_xml: Path,
    bands: Sequence[str],
    *,
    app_version: str,
    aux_source: str,
) -> None:
    """Append LaSRC output bands to an ESPA XML in place

    Appends one ``<band>`` for each name in ``bands``, in order, followed by
    ``sr_aerosol`` and ``sr_aerosol_qa``. ``bands`` holds ``sr_band*`` names
    and, for Landsat, whichever of ``toa_band9``, ``bt_band10`` and
    ``bt_band11`` were written. C writes the toa/bt bands before ``sr_band*``
    and output order follows ``bands``, so callers must pass the toa/bt names
    before the ``sr_band*`` names to reproduce C's band order, which is also the
    HDF SDS order. Raises ``ValueError`` if any band to be added already exists
    and ``FileNotFoundError`` if any band's ``.img`` is missing next to the XML,
    before the XML is changed.
    ``app_version`` is the ``LaSRC_<version>``
    string and ``aux_source`` the base name of the auxiliary climate file, which
    C records as ``source`` on the ``sr_band*`` bands.
    """
    espa_xml = Path(espa_xml).resolve()
    mm = Metadata(xml_filename=str(espa_xml))
    mm.parse()
    root = mm.xml_object
    bands_el = root.bands
    ns = etree.QName(bands_el).namespace or ""

    existing = {str(b.get("name")) for b in bands_el.iterchildren()}
    duplicates = sorted(existing & {*bands, AERO_BAND, AERO_QA_BAND})
    if duplicates:
        raise ValueError(f"Bands already in {espa_xml}: {', '.join(duplicates)}")

    def q(tag: str) -> str:
        return f"{{{ns}}}{tag}"

    product_id = str(root.global_metadata.product_id)
    missing = [
        f"{product_id}_{name}.img"
        for name in (*bands, AERO_BAND, AERO_QA_BAND)
        if not (espa_xml.parent / f"{product_id}_{name}.img").exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing LaSRC outputs in {espa_xml.parent}: {', '.join(missing)}"
        )
    is_sentinel = str(root.global_metadata.satellite).startswith("Sentinel")

    if is_sentinel:
        # Size comes from band 2 (10 m); short_name and level1_filename from
        # band 1, as in LaSRC input.c and output.c
        geometry = _find_band(bands_el, "B02", "MSIL1C")
        naming = _find_band(bands_el, "B01", "MSIL1C")
        n_keep = 7
        aero_qa_bits = _SENTINEL_AERO_QA_BITS
        l1_filename = naming.findtext(q("level1_filename"))
    else:
        geometry = naming = _find_band(bands_el, "b1", "L1")
        n_keep = 4
        aero_qa_bits = _LANDSAT_AERO_QA_BITS
        l1_filename = None

    prefix = _short_name(naming, ns, n_keep)
    production_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    pixel_size = geometry.find(q("pixel_size"))
    if pixel_size is None:
        raise ValueError(f"Band {geometry.get('name')} has no pixel_size")
    refl_range = _valid_range(
        enc.MIN_VALID_REFL, enc.MAX_VALID_REFL, enc.SCALE_FACTOR_REFL, enc.OFFSET_REFL
    )
    th_range = _valid_range(
        enc.MIN_VALID_TH, enc.MAX_VALID_TH, enc.SCALE_FACTOR_TH, enc.OFFSET_TH
    )
    aero_range = _valid_range(
        enc.MIN_VALID_AERO, enc.MAX_VALID_AERO, enc.SCALE_FACTOR_AERO, 0.0
    )

    def add(
        name: str,
        *,
        product: str,
        source: str | None = None,
        category: str,
        data_type: str,
        fill_value: int,
        scale_offset: tuple[float, float] | None,
        short_suffix: str,
        long_name: str,
        data_units: str,
        valid_range: tuple[float, float] | None,
        bits: Sequence[str] = (),
        level1_filename: str | None = None,
    ) -> None:
        attrs = {"product": product}
        if source is not None:
            attrs["source"] = source
        attrs.update(
            name=name,
            category=category,
            data_type=data_type,
            nlines=str(geometry.get("nlines")),
            nsamps=str(geometry.get("nsamps")),
            fill_value=str(fill_value),
        )
        if scale_offset is not None:
            attrs["scale_factor"] = f"{scale_offset[0]:10.8f}"
            attrs["add_offset"] = f"{scale_offset[1]:f}"
        # Built as plain elements; objectify elements do not allow setting text
        band = etree.Element(q("band"), attrib=attrs)

        def child(
            parent: etree._Element, tag: str, text: str | None = None
        ) -> etree._Element:
            el = etree.SubElement(parent, q(tag))
            el.text = text
            return el

        child(band, "short_name", prefix + short_suffix)
        child(band, "long_name", long_name)
        child(band, "file_name", f"{product_id}_{name}.img")
        ps = child(band, "pixel_size")
        ps.set("x", str(pixel_size.get("x")))
        ps.set("y", str(pixel_size.get("y")))
        ps.set("units", "meters")
        child(band, "resample_method", "none")
        child(band, "data_units", data_units)
        if valid_range is not None:
            vr = child(band, "valid_range")
            vr.set("min", f"{valid_range[0]:f}")
            vr.set("max", f"{valid_range[1]:f}")
        if bits:
            bd = child(band, "bitmap_description")
            for num, text in enumerate(bits):
                child(bd, "bit", text).set("num", str(num))
        child(band, "app_version", app_version)
        if level1_filename is not None:
            child(band, "level1_filename", level1_filename)
        child(band, "production_date", production_date)
        bands_el.append(band)

    for name in bands:
        if name.startswith("sr_band"):
            number = name.removeprefix("sr_band")
            add(
                name,
                product="sr_refl",
                source=aux_source,
                category="image",
                data_type="UINT16",
                fill_value=enc.FILL_VALUE,
                scale_offset=(enc.SCALE_FACTOR_REFL, enc.OFFSET_REFL),
                short_suffix="SR",
                long_name=f"band {number} surface reflectance",
                data_units="reflectance",
                valid_range=refl_range,
                level1_filename=l1_filename,
            )
        elif name.startswith("toa_band"):
            number = name.removeprefix("toa_band")
            add(
                name,
                product="toa_refl",
                category="image",
                data_type="UINT16",
                fill_value=enc.FILL_VALUE,
                scale_offset=(enc.SCALE_FACTOR_REFL, enc.OFFSET_REFL),
                short_suffix="TOA",
                long_name=f"band {number} top-of-atmosphere reflectance",
                data_units="reflectance",
                valid_range=refl_range,
            )
        elif name.startswith("bt_band"):
            number = name.removeprefix("bt_band")
            add(
                name,
                product="toa_bt",
                category="image",
                data_type="UINT16",
                fill_value=enc.FILL_VALUE,
                scale_offset=(enc.SCALE_FACTOR_TH, enc.OFFSET_TH),
                short_suffix="BT",
                long_name=f"band {number} top-of-atmosphere brightness temperature",
                data_units="temperature (kelvin)",
                valid_range=th_range,
            )
        else:
            raise ValueError(f"Unsupported LaSRC band name: {name}")

    add(
        AERO_BAND,
        product="sr_refl",
        category="image",
        data_type="INT16",
        fill_value=enc.FILL_VALUE_AERO,
        scale_offset=(enc.SCALE_FACTOR_AERO, 0.0),
        short_suffix="AERO",
        long_name="surface reflectance aerosol optical thickness",
        data_units="unitless",
        valid_range=aero_range,
    )
    add(
        AERO_QA_BAND,
        product="sr_refl",
        category="qa",
        data_type="UINT8",
        fill_value=1,
        scale_offset=None,
        short_suffix="AEROQA",
        long_name="surface reflectance aerosol quality assessment",
        data_units="quality/feature classification",
        valid_range=None,
        bits=aero_qa_bits,
    )

    # Same steps as espa.Metadata.write, without its cwd-relative temp name
    mm.validate()
    tmp = espa_xml.with_name(f"temp_ESPA_XML_{espa_xml.name}")
    with open(tmp, "wb") as fd:
        fd.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
        fd.write(etree.tostring(mm.xml_object, encoding="utf-8", pretty_print=True))
    os.replace(tmp, espa_xml)
