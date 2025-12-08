# syntax=docker/dockerfile:1.7-labs
ARG PLATFORM=linux/amd64
FROM --platform=${PLATFORM} ghcr.io/prefix-dev/pixi:bookworm-slim AS build

WORKDIR /app

RUN apt update && \
    apt install -y --no-install-recommends \
        git \
        ca-certificates \
        && \
    rm -rf /var/lib/apt/lists/*

COPY --parents pixi.toml pixi.lock packages /app/


# FIXME: can we install in a detached prefix to make mounting /app easier?
# e.g.,
# pixi config set --global detached-environments "/opt/pixi/envs" && \
#
# The trouble is now the PREFIX is unpredictable. We can find it via CONDA_PREFIX, but
# we'd need to set the INC/LIB paths in a script
RUN --mount=type=cache,target=/root/.cache/rattler/cache,sharing=private \
# #    pixi self-update && \
    pixi install --locked

ENV PREFIX=/app/.pixi/envs/default

RUN echo '#!/bin/bash' > /app/entrypoint.sh && \
    pixi shell-hook -e default -s bash >> /app/entrypoint.sh && \
    echo 'exec "$@"' >> /app/entrypoint.sh

# ----- HLS Python utilities
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
FROM --platform=${PLATFORM} debian:bookworm-slim AS prod

WORKDIR /app

COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build --chmod=0755 /app/entrypoint.sh /app/entrypoint.sh

ENV LD_LIBRARY_PATH="/app/.pixi/envs/default/lib"

ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD [ "/bin/bash", "-c" ]
