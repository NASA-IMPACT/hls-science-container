#!/bin/bash

# Exit on any error
set -o errexit

hls-nextgen-orchestration sentinel
exit $?
