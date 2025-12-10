#!/bin/bash
# This is a modified and simplified version of the "run_Fmask_4_7.sh" that
# is included in the Fmask 4.7 MCR-based release.
#
# The setting of LD_LIBRARY_PATH has already been done by the "activate.sh"
# script, so we just need to pass the location of the MCR and auxiliary data.

FMASK_ROOT=${CONDA_PREFIX}/bin/fmask/

${FMASK_ROOT}/application/Fmask_4_7 ${FMASK_ROOT}/application/AuxiData/
