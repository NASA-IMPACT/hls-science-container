#!/bin/bash

set -e

INSTALLER_FILE="Fmask_Linux_mcr.install"
chmod +x $INSTALLER_FILE
./$INSTALLER_FILE -destinationFolder "$PREFIX/bin/fmask4" -agreeToLicense yes -mode silent

# NB: The "run_Fmask_{version}.sh" takes care of setting LD_LIBRARY_PATH relative to
# the MCRROOT (and indeed will override any LD_LIBRARY_PATH we set).
#
# Here we add a shell script to help point the "run_Fmask_{version}.sh" to the
# correct MCR root and auxiliary data directories.
cat <<EOF >> "${PREFIX}/bin/run_Fmask.sh"
${CONDA_PREFIX}/bin/fmask4/application/run_Fmask_4_7.sh ${CONDA_PREFIX}/bin/fmask4/v912 ${CONDA_PREFIX}/bin/fmask4/application/AuxiData/
EOF
