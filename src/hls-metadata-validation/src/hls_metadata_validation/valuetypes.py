"""Check string values against spec attribute types."""

from __future__ import annotations

import re

from hls_metadata_validation.spec import AttributeType

DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def matches_type(value: str, type_: AttributeType) -> bool:
    """Return whether ``value`` is a valid textual ``type_`` value."""
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
