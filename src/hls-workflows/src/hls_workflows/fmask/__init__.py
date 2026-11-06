"""Standalone "just-Fmask" pipeline for Fmask performance testing.

A debug-oriented pipeline that runs only the Fmask step (Download -> Fmask ->
Upload) so changes to Fmask can be measured in isolation from the rest of the
production workflow. Unlike production, it does not skip granules with an
invalid solar zenith angle.
"""

from .workflow import construct_pipeline

__all__ = ["construct_pipeline"]
