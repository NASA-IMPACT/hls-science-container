ARG PLATFORM=linux/amd64
FROM --platform=${PLATFORM} ghcr.io/prefix-dev/pixi:bookworm-slim AS build

WORKDIR /app

RUN apt update && \
    apt install -y --no-install-recommends \
        git \
        ca-certificates \
        && \
    rm -rf /var/lib/apt/lists/*

COPY pixi.toml pixi.lock /app/


# FIXME: build espa-product-formatter and espa-surface-reflectance via rattler-build
# https://pixi.sh/v0.43.0/build/advanced_cpp/#testing-if-everything-works

# FIXME: can we install in a detached prefix to make mounting /app easier?
# e.g.,
# pixi config set --global detached-environments "/opt/pixi/envs" && \
#
# The trouble is now the PREFIX is unpredictable. We can find it via CONDA_PREFIX, but
# we'd need to set the INC/LIB paths in a script
RUN --mount=type=cache,target=/root/.cache/rattler/cache,sharing=private \
#    pixi self-update && \
    pixi install --locked

ENV PREFIX=/app/.pixi/envs/default

ENV ZLIB_PREFIX=${PREFIX} \
    XZ_PREFIX=${PREFIX} \
    SZIP_PREFIX=${PREFIX} \
    PNG_PREFIX=${PREFIX} \
    FREETYPE2_PREFIX=${PREFIX} \
    XML2_PREFIX=${PREFIX} \
    XSLT_PREFIX=${PREFIX} \
    IDN_PREFIX=${PREFIX}/idn \
    CURL_PREFIX=${PREFIX}/curl \
    JPEG_PREFIX=${PREFIX} \
    JBIG_PREFIX=${PREFIX} \
    TIFF_PREFIX=${PREFIX} \
    GEOTIFF_PREFIX=${PREFIX} \
    HDF4_PREFIX=${PREFIX} \
    HDF5_PREFIX=${PREFIX} \
    NETCDF4_PREFIX=${PREFIX} \
    HDFEOS_PREFIX=${PREFIX} \
    HDFEOS5_PREFIX=${PREFIX} \
    PROJ4_PREFIX=${PREFIX} \
    GDAL_PREFIX=${PREFIX} \
    ESPA_PREFIX=/opt/espa \
    LASRC_PREFIX=/opt/lasrc \
    HLS_LIBS_PREFIX=/opt/hls

ENV ZLIBINC=$ZLIB_PREFIX/include \
    ZLIBLIB=$ZLIB_PREFIX/lib \
    XZINC=$XZ_PREFIX/include \
    XZLIB=$XZ_PREFIX/lib \
    LZMAINC=$XZ_PREFIX/include \
    LZMALIB=$XZ_PREFIX/lib \
    SZIPINC=$SZIP_PREFIX/include \
    SZIPLIB=$SZIP_PREFIX/lib \
    PNGINC=$PNG_PREFIX/include \
    PNGLIB=$PNG_PREFIX/lib \
    FREETYPE2INC=$FREETYPE2_PREFIX/include \
    FREETYPE2LIB=$FREETYPE2_PREFIX/lib \
    XML2INC=$XML2_PREFIX/include/libxml2 \
    XML2LIB=$XML2_PREFIX/lib \
    XSLTINC=$XSLT_PREFIX/include/libxslt \
    XSLTLIB=$XSLT_PREFIX/lib \
    IDNINC=$IDN_PREFIX/include \
    IDNLIB=$IDN_PREFIX/lib \
    CURLINC=$CURL_PREFIX/include \
    CURLLIB=$CURL_PREFIX/lib \
    JPEGINC=$JPEG_PREFIX/include \
    JPEGLIB=$JPEG_PREFIX/lib \
    JBIGINC=$JBIG_PREFIX/include \
    JBIGLIB=$JBIG_PREFIX/lib \
    TIFFINC=$TIFF_PREFIX/include \
    TIFFLIB=$TIFF_PREFIX/lib \
    GEOTIFFINC=$GEOTIFF_PREFIX/include \
    GEOTIFFLIB=$GEOTIFF_PREFIX/lib \
    GEOTIFF_INC=$GEOTIFF_PREFIX/include \
    GEOTIFF_LIB=$GEOTIFF_PREFIX/lib \
    HDFINC=$HDF4_PREFIX/include \
    HDFLIB=$HDF4_PREFIX/lib \
    HDF4INC=$HDF4_PREFIX/include \
    HDF4LIB=$HDF4_PREFIX/lib \
    HDF5INC=$HDF5_PREFIX/include \
    HDF5LIB=$HDF5_PREFIX/lib \
    NCDF4INC=$NETCDF4_PREFIX/include \
    NCDF4LIB=$NETCDF4_PREFIX/lib \
    NETCDF4INC=$NETCDF4_PREFIX/include \
    NETCDF4LIB=$NETCDF4_PREFIX/lib \
    HDFEOSINC=$HDFEOS_PREFIX/include \
    HDFEOSLIB=$HDFEOS_PREFIX/lib \
    HDFEOS_INC=$HDFEOS_PREFIX/include \
    HDFEOS_LIB=$HDFEOS_PREFIX/lib \
    HDFEOS5_INC=$HDFEOS5_PREFIX/include \
    HDFEOS5_LIB=$HDFEOS5_PREFIX/lib \
    HDFEOS_GCTPINC=$HDFEOS_PREFIX/include \
    HDFEOS_GCTPLIB=$HDFEOS_PREFIX/lib \
    GCTPINC=$HDFEOS_PREFIX/include \
    GCTPLIB=$HDFEOS_PREFIX/lib \
    PROJ4_INC=${PROJ4_PREFIX}/include \
    PROJ4_LIB=${PROJ4_PREFIX}/lib \
    PROJINC=${PROJ4_PREFIX}/include \
    PROJLIB=${PROJ4_PREFIX}/lib \
    GDAL_INC=$GDAL_PREFIX/include \
    GDAL_LIB=$GDAL_PREFIX/lib \
    ESPALIB=${ESPA_PREFIX}/lib \
    ESPAINC=${ESPA_PREFIX}/include

RUN echo '#!/bin/bash' > /app/entrypoint.sh && \
    pixi shell-hook -e default -s bash >> /app/entrypoint.sh && \
    echo "export ESPA_PREFIX=${ESPA_PREFIX}" >> /app/entrypoint.sh && \
    echo "export LASRC_PREFIX=${LASRC_PREFIX}" >> /app/entrypoint.sh && \
    echo "export HLS_LIBS_PREFIX=${HLS_LIBS_PREFIX}" >> /app/entrypoint.sh && \
    echo 'export PATH=${ESPA_PREFIX}/bin:${LASRC_PREFIX}/bin:${HLS_LIBS_PREFIX}/bin:${PATH}' >> /app/entrypoint.sh && \
    echo 'exec "$@"' >> /app/entrypoint.sh

# TODO: upstream patch espa-product-formatter to include `-std=gnu90`
COPY patches/espa-product-formatter-std-gnu90.patch /tmp/
RUN REPO_NAME=espa-product-formatter && \
    git clone --depth 1 -b 3.5.0 https://github.com/NASA-IMPACT/${REPO_NAME}.git /tmp/${REPO_NAME} && \
    cd /tmp/${REPO_NAME} && \
    git apply ../espa-product-formatter-std-gnu90.patch && \
    . /app/entrypoint.sh && \
    make -j4 && \
    PREFIX=${ESPA_PREFIX} make install && \
    cd /tmp && rm -rf /tmp/*

RUN REPO_NAME=espa-surface-reflectance && \
    git clone --depth 1 -b v3.5.1.0 https://github.com/NASA-IMPACT/${REPO_NAME}.git /tmp/${REPO_NAME} && \
    cd /tmp/${REPO_NAME} && \
    . /app/entrypoint.sh && \
    make -j4 ENABLE_THREADING=yes && \
    PREFIX=${LASRC_PREFIX} make install && \
    cd /tmp && rm -rf /tmp/*

COPY src/hls-libs /tmp/hls-libs
RUN cd /tmp/hls-libs && \
    . /app/entrypoint.sh && \
    GCTPLINK="-lGctp -lm" \
        HDFLINK="-lmfhdf -ldf -lm" \
        SRC_DIR=/tmp/hls-libs/common \
        CC="gcc" \
        CFLAGS="-std=gnu90" \
        make && \
    make install && \
    cd /tmp && rm -rf /tmp/hls-libs/

# FIXME: install AWS CLI v2

# FIXME: install from Conda or PyPI
# click==7.1.2
# rio-cogeo==1.1.10 --no-binary rasterio --user
# git+https://github.com/NASA-IMPACT/libxml2-python3
# ... or just let the Git sources below resolve them

# FIXME: install the Git sources below:
# git+https://github.com/NASA-IMPACT/hls-thumbnails@v1.3
# git+https://github.com/NASA-IMPACT/hls-metadata@v2.7
# git+https://github.com/NASA-IMPACT/hls-manifest@v2.1
# git+https://github.com/NASA-IMPACT/hls-browse_imagery@v1.7
# git+https://github.com/NASA-IMPACT/hls-hdf_to_cog@v2.1
# git+https://github.com/NASA-IMPACT/hls-utilities@v1.11.1
# ^^^ need to relax NumPy and probably rasterio
# git+https://github.com/NASA-IMPACT/hls-cmr_stac@v1.7
# git+https://github.com/NASA-IMPACT/hls-vi@v1.17

# -----

# FIXME: uncomment to "productionize" pixi build
# "Productionize" pixi install: https://pixi.sh/latest/deployment/container/
# FROM --platform=${PLATFORM} debian:bookworm-slim AS prod
# 
# WORKDIR /app
# 
# COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
# COPY --from=build --chmod=0755 /app/entrypoint.sh /app/entrypoint.sh
# COPY --from=build /opt/ /opt/
# 
# ENV ESPA_SCHEMA="/opt/espa/schema/espa_internal_metadata_v2_2.xsd"
# ENV LD_LIBRARY_PATH="/app/.pixi/envs/default/lib"
# 
# ENTRYPOINT [ "/app/entrypoint.sh" ]
# CMD [ "/bin/bash", "-c" ]
