#!/bin/bash
set -euo pipefail

export SOURCE_DATE_EPOCH=0

# lasrc_py is a Cargo workspace member depending on the path crate
# ../lasrc_core, so build from the subdir; the full repo (including the
# committed Cargo.lock) is present in the source checkout. maturin is the
# build backend declared in lasrc_py/pyproject.toml.
cd lasrc_py
${PYTHON} -m pip install . --no-deps --no-build-isolation -vv
