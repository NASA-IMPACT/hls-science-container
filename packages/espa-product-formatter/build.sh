#!/bin/bash
set -euo pipefail

export SOURCE_DATE_EPOCH=0
export TIFFINC="${PREFIX}/include"
export TIFFLIB="${PREFIX}/lib"
export GEOTIFF_INC="${PREFIX}/include"
export GEOTIFF_LIB="${PREFIX}/lib"
export HDFINC="${PREFIX}/include"
export HDFLIB="${PREFIX}/lib"
export HDF5INC="${PREFIX}/include"
export HDF5LIB="${PREFIX}/lib"
export HDFEOS_INC="${PREFIX}/include"
export HDFEOS_LIB="${PREFIX}/lib"
export HDFEOS_GCTPINC="${PREFIX}/include"
export HDFEOS_GCTPLIB="${PREFIX}/lib"
export HDFEOS5_LIB="${PREFIX}/lib"
export HDFEOS5_INC="${PREFIX}/include"
export NCDF4INC="${PREFIX}/include"
export NCDF4LIB="${PREFIX}/lib"
export JPEGINC="${PREFIX}/include"
export JPEGLIB="${PREFIX}/lib"
export XML2INC="${PREFIX}/include/libxml2"
export XML2LIB="${PREFIX}/lib"
export JBIGINC="${PREFIX}/include"
export JBIGLIB="${PREFIX}/lib"
export ZLIBINC="${PREFIX}/include"
export ZLIBLIB="${PREFIX}/lib"
export SZIPINC="${PREFIX}/include"
export SZIPLIB="${PREFIX}/lib"
export CURLINC="${PREFIX}/include"
export CURLLIB="${PREFIX}/lib"
export LZMAINC="${PREFIX}/include"
export LZMALIB="${PREFIX}/lib"
export IDNINC="${PREFIX}/include"
export IDNLIB="${PREFIX}/lib"
export ESPAINC="${PREFIX}/include"
export ESPALIB="${PREFIX}/lib"

make -j${CPU_COUNT}
make install

cp $RECIPE_DIR/activate.sh $PREFIX/etc/conda/activate.d/espa-product-formatter.sh
cp $RECIPE_DIR/deactivate.sh $PREFIX/etc/conda/deactivate.d/espa-product-formatter.sh
