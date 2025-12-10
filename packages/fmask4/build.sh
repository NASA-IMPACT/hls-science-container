#!/bin/bash

set -e

INSTALL_DIR="$PREFIX/bin/fmask4"

INSTALLER_FILE="Fmask_Linux_mcr.install"
chmod +x $INSTALLER_FILE
./$INSTALLER_FILE -destinationFolder ${INSTALL_DIR} -agreeToLicense yes -mode silent

cp ${RECIPE_DIR}/run_Fmask.sh ${PREFIX}/bin/
chmod +x ${PREFIX}/bin/run_Fmask.sh

mkdir -p $PREFIX/etc/conda/activate.d/
cp $RECIPE_DIR/activate.sh $PREFIX/etc/conda/activate.d/fmask4.sh
mkdir -p $PREFIX/etc/conda/deactivate.d/
cp $RECIPE_DIR/deactivate.sh $PREFIX/etc/conda/deactivate.d/fmask4.sh
