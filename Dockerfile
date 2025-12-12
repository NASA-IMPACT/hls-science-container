# syntax=docker/dockerfile:1.7-labs
ARG PLATFORM=linux/amd64
FROM --platform=${PLATFORM} ghcr.io/prefix-dev/pixi:bookworm-slim AS build

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        && \
    rm -rf /var/lib/apt/lists/*

COPY --parents pixi.toml pixi.lock packages /app/

# RUN --mount=type=cache,target=/root/.cache/rattler/cache,sharing=private \
#     pixi install --frozen
RUN pixi install --frozen

ENV PREFIX=/app/.pixi/envs/default

RUN echo '#!/bin/bash' > /app/entrypoint.sh && \
    pixi shell-hook --frozen -e default -s bash >> /app/entrypoint.sh && \
    echo 'exec "$@"' >> /app/entrypoint.sh

# "Productionize" pixi install: https://pixi.sh/latest/deployment/container/
FROM --platform=${PLATFORM} debian:bookworm-slim AS prod

# Enforce v1.0.0 for STAC specification within PySTAC
ENV PYSTAC_STAC_VERSION_OVERRIDE=1.0.0
# Allow GDAL to open MEM dataset with a datapointer (required for hls-hdf_to_cog)
ENV GDAL_MEM_ENABLE_OPEN=YES

WORKDIR /app

COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build --chmod=0755 /app/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD [ "/bin/bash", "-c" ]
