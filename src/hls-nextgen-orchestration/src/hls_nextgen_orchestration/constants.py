from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# HLS products (reflectance & vegetation indexes)
PRODUCTS = Literal["HLS", "HLS-VI"]


@dataclass(frozen=True, kw_only=True)
class HlsVersion:
    """HLS product version"""

    major: int = 2
    minor: int = 0

    def to_str(self) -> str:
        """Convert into a string (e.g., v2.0)"""
        return f"v{self.major}.{self.minor}"

    @classmethod
    def from_str(cls, version: str) -> HlsVersion:
        """Parse from a string (e.g., v2.0)"""
        parts = version.lstrip("v").split(".")
        return cls(
            major=int(parts[0]),
            minor=int(parts[1]),
        )


HLS_VERSION = HlsVersion(major=2, minor=0)
