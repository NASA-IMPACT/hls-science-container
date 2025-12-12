#!/bin/bash
set -e

CONDA_CHANNEL=${CONDA_CHANNEL:-s3://hls-conda-channels/hls-atmospheric-correction}
CHANNEL_DIR="channel"
PIXI_ENV="build"

export PIXI_FROZEN="true"

command -v pixi > /dev/null 2>&1 || { echo >&2 "pixi is required. Aborting."; exit 1; }

echo "Using '$PIXI_ENV' environment from pixi.toml..."

# 1. Determine targets
echo "🔍 Determining build targets..."
BUILD_LIST=$(pixi run -e $PIXI_ENV determine-targets "$@")

if [[ -z "${BUILD_LIST// }" ]]; then
    echo "✅ No packages to build."
    exit 0
fi

# 2. Build loop
echo "----------------------------------------------------"
for pkg in $BUILD_LIST; do
    echo "📦 Building $pkg..."
    
    # Run the build command inside the env
    pixi run -e $PIXI_ENV -- rattler-build build \
        --recipe "packages/$pkg/recipe.yml" \
        --skip-existing=all \
        --output-dir "$CHANNEL_DIR" \
        -c conda-forge \
        --test native
done

# 3. Upload to channel
echo "----------------------------------------------------"
read -p "❓ Sync to S3? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pixi run -e $PIXI_ENV -- \
        rattler-build upload s3 \
            --force \
            --channel $CONDA_CHANNEL \
            ${CHANNEL_DIR}/**/*.conda
    pixi run -e $PIXI_ENV -- rattler-index s3 $CONDA_CHANNEL
fi

# 4. Update lockfile
echo "----------------------------------------------------"
read -p "❓ Update pixi.lock? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pixi update $BUILD_LIST
fi

# 5. Docker build
echo "----------------------------------------------------"
read -p "❓ Rebuild Docker? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker build -t hls-atmospheric-correction:latest .
fi
