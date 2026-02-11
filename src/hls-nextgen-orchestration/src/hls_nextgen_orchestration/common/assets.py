from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessingMetadata:
    """
    Metadata derived from parsing the granule, used for naming outputs.
    """

    output_name: str
    bucket_key: str
