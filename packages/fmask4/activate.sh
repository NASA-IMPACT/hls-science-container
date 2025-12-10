#!/bin/bash

# We can't rewrite the linked paths in the MCR binaries, so we instead set LD_LIBRARY_PATH
# as part of activating the environment.
export LD_LIBRARY_PATH_FMASK4=$LD_LIBRARY_PATH

export FMASK_ROOT=${CONDA_PREFIX}/bin/fmask
MCR_ROOT=${FMASK_ROOT}/v912

# MCR link paths
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${MCR_ROOT}/runtime/glnxa64
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${MCR_ROOT}/bin/glnxa64
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${MCR_ROOT}/sys/os/glnxa64

# We also need to make sure libXt.so from `xorg-libxt` is linked
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${CONDA_PREFIX}/lib

export LD_LIBRARY_PATH
