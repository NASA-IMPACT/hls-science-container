from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


# ----- Asset type definitions
# We can't use `isinstance` on generics like `list[str]`, but we can
# define a subclass to allow type hints AND runtime checks.
class Paths(list[Path]): ...


@dataclass(frozen=True)
class S3Path:
    """An S3 location split into its ``bucket`` and ``key``."""

    bucket: str
    key: str

    @property
    def uri(self) -> str:
        """Full ``s3://bucket/key`` URI."""
        return f"s3://{self.bucket}/{self.key}"

    def __truediv__(self, suffix: str) -> S3Path:
        """Return a new ``S3Path`` with ``suffix`` appended to the key.

        Lets paths be built up incrementally, e.g. ``config.gibs_path / gibs_id``.
        """
        return S3Path(self.bucket, f"{self.key.rstrip('/')}/{str(suffix).strip('/')}")

    def __str__(self) -> str:
        return self.uri
