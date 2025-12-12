#!/bin/bash
set -e

# scripts/build_local.sh
CONDA_CHANNEL="s3://hls-conda-channels/hls-atmospheric-correction"
CHANNEL_DIR="channel"
REGION="us-west-2"
PIXI_ENV="build"

# We only check for pixi. Everything else is handled by the $PIXI_ENV environment.
command -v pixi > /dev/null 2>&1 || { echo >&2 "pixi is required. Aborting."; exit 1; }

echo "Using '$PIXI_ENV' environment from pixi.toml..."

# 1. Determine Targets
# We use 'pixi run -e $PIXI_ENV' to execute the python script using the project's python/pyyaml
echo "🔍 Determining build targets..."
BUILD_LIST=$(pixi run --frozen -e $PIXI_ENV determine-targets "$@")

if [[ -z "${BUILD_LIST// }" ]]; then
    echo "✅ No packages to build."
    exit 0
fi

# 2. Sync
echo "⬇️  Syncing existing channel..."
mkdir -p "$CHANNEL_DIR/linux-64"
# We use 'pixi run -e $PIXI_ENV -- aws' to ensure we use the project's AWS CLI version
pixi run --frozen -e $PIXI_ENV -- aws s3 sync "$CONDA_CHANNEL/linux-64" "$CHANNEL_DIR/linux-64" --region "$REGION" --delete || true

# 3. Build Loop
echo "----------------------------------------------------"
for pkg in $BUILD_LIST; do
    echo "📦 Building $pkg..."
    
    # Run the build command inside the env
    pixi run --frozen -e $PIXI_ENV -- rattler-build build \
        --recipe "packages/$pkg/recipe.yml" \
        --output-dir "$CHANNEL_DIR" \
        -c "./$CHANNEL_DIR" \
        -c conda-forge \
        --experimental
        
    pixi run --frozen -e $PIXI_ENV -- rattler-index fs "$CHANNEL_DIR"
done

# 4. Upload Prompt
echo "----------------------------------------------------"
read -p "❓ Sync to S3? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pixi run --frozen -e $PIXI_ENV -- \
        aws s3 sync "$CHANNEL_DIR/linux-64" "$CONDA_CHANNEL/linux-64"
fi

# 5. Update prompt
echo "----------------------------------------------------"
read -p "❓ Update pixi.lock and build Docker? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pixi update $BUILD_LIST
fi

# 6. Docker build prompt
echo "----------------------------------------------------"
read -p "❓ Rebuild Docker? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker build -t hls-atmospheric-correction:latest .
fi
