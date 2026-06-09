#!/bin/bash

# Exit on any error
set -o errexit

hls-nextgen-orchestration landsat-ac
exit $?
