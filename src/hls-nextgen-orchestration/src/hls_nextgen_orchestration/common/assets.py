from dataclasses import dataclass
from pathlib import Path


# ----- Asset type definitions
# We can't use `isinstance` on generics like `list[str]`, but we can
# define a subclass to allow type hints AND runtime checks.
class Paths(list[Path]): ...


# ----- Data containers
@dataclass(frozen=True)
class ProcessingMetadata:
    """
    Metadata derived from parsing the granule, used for naming outputs.
    """

    output_name: str
    bucket_key: str
