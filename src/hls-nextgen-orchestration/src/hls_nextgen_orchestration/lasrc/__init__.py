"""Standalone "just-LaSRC" pipeline for C vs Rust intercomparison.

This is a temporary, debug-oriented pipeline that runs only the LaSRC step
(Download -> LaSRC -> Upload) so the C LaSRC and the Rust ``lasrc-rs`` port can
be compared on performance and accuracy. It intentionally does NOT run the full
production workflow (the downstream to/from-HDF steps don't support the Rust
output yet). Once the Rust path is validated and wired into the main pipeline,
this module and the ``--lasrc-version`` flag are expected to be removed.
"""

from .workflow import construct_pipeline

__all__ = ["construct_pipeline"]
