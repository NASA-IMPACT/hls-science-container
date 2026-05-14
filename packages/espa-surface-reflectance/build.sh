#!/bin/bash
set -euo pipefail

export SOURCE_DATE_EPOCH=0
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
export GCTPINC="${PREFIX}/include"
export GCTPLIB="${PREFIX}/lib"
export JPEGINC="${PREFIX}/include"
export JPEGLIB="${PREFIX}/lib"
export LZMAINC="${PREFIX}/include"
export LZMALIB="${PREFIX}/lib"
export SZIPINC="${PREFIX}/include"
export SZIPLIB="${PREFIX}/lib"
export XML2INC="${PREFIX}/include/libxml2"
export XML2LIB="${PREFIX}/lib"
export ZLIBINC="${PREFIX}/include"
export ZLIBLIB="${PREFIX}/lib"
export ESPAINC="${PREFIX}/include"
export ESPALIB="${PREFIX}/lib"

make -j${CPU_COUNT} ENABLE_THREADING=yes all-lasrc
make install-lasrc
