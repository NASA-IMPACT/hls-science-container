#!/bin/bash
# shellcheck disable=SC2153
# shellcheck disable=SC1091

# Exit on any error
set -o errexit

hls-nextgen-orchestration sentinel
exit $?
