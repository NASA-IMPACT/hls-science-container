"""Check string values against spec attribute types."""

from __future__ import annotations

import re

from hls_metadata_validation.spec import AttributeType

DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

# Granules built from >1 input scene join per-scene values into one string: Landsat
# WRS-2 mosaics use "a; b" (hls-libs lsatmeta.c), Sentinel-2 twin granules use "a + b"
# (hls-libs consolidate.c). metadata_creator splits ";" into separate CMR <Value>s but
# leaves "+" joined. "+" must be space-padded so signs/exponents ("+1", "1e+5") survive.
MULTI_VALUE_SEP = re.compile(r";|\s\+\s")


def matches_type(value: str, type_: AttributeType) -> bool:
    """Return whether every per-scene part of ``value`` is a valid ``type_`` value."""
    return all(_matches_one(part, type_) for part in MULTI_VALUE_SEP.split(value))


def _matches_one(value: str, type_: AttributeType) -> bool:
    text = value.strip()
    if type_ == "string":
        return True
    if type_ == "datetime":
        return DATETIME_RE.match(text) is not None
    try:
        number = float(text)
    except ValueError:
        return False
    if type_ == "integer":
        return number.is_integer()
    return True
