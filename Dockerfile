# syntax=docker/dockerfile:1.7-labs
ARG PLATFORM=linux/amd64
FROM --platform=${PLATFORM} ghcr.io/prefix-dev/pixi:bookworm-slim AS build

WORKDIR /app

RUN apt update && \
    apt install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        unzip \
        wget \
        && \
    rm -rf /var/lib/apt/lists/*

# ----- Install Fmask4
ARG FMASK_INSTALLER=https://fmask4installer.s3.amazonaws.com/Fmask_4_7_issue40_Linux_mcr.install
RUN wget -q -O /tmp/Fmask.install ${FMASK_INSTALLER} && \
    chmod +x /tmp/Fmask.install && \
    mkdir -p /opt && \
    /tmp/Fmask.install -destinationFolder /opt/fmask -agreeToLicense yes -mode silent && \
    rm /tmp/Fmask.install

# ----- Install package dependencies
COPY --parents pixi.toml pixi.lock packages /app/
RUN --mount=type=cache,target=/root/.cache/rattler/cache,sharing=private \
    pixi install --frozen
ENV PREFIX=/app/.pixi/envs/default
RUN echo '#!/bin/bash' > /app/entrypoint.sh && \
    pixi shell-hook --frozen -e default -s bash >> /app/entrypoint.sh && \
    echo 'exec "$@"' >> /app/entrypoint.sh

# ===== Production installation
# "Productionize" pixi install: https://pixi.sh/latest/deployment/container/
FROM --platform=${PLATFORM} debian:bookworm-slim AS prod

# install libxt for MCR / Fmask
RUN apt update && \
    apt install -y --no-install-recommends \
        libxt6 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Enforce v1.0.0 for STAC specification within PySTAC
ENV PYSTAC_STAC_VERSION_OVERRIDE=1.0.0
# Allow GDAL to open MEM dataset with a datapointer (required for hls-hdf_to_cog)
ENV GDAL_MEM_ENABLE_OPEN=YES
ENV FMASK_PREFIX=/opt/fmask

COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build --chmod=0755 /app/entrypoint.sh /app/entrypoint.sh
COPY --from=build /opt/fmask ${FMASK_PREFIX}
COPY packages/fmask4/run_Fmask.sh /app/.pixi/envs/default/bin
COPY src/scripts/*.sh /app/.pixi/envs/default/bin

ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD [ "/bin/bash", "-c" ]
