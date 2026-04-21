#!/bin/bash

# Exit on any error
set -o errexit
set -x

python -m "hls_nextgen_orchestration.landsat_tile.workflow"
