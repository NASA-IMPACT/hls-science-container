#!/bin/bash

# Exit on any error
set -o errexit
hls-workflows landsat-tile
exit $?
