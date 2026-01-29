#!/bin/bash

# Exit on any error
set -o errexit

python -m "hls_nextgen_orchestration.landsat_ac.workflow"
