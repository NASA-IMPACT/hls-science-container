#!/bin/bash
set -e

# scripts/build_and_publish.sh
SORTED_PACKAGES=$1
CONDA_CHANNEL="s3://hls-conda-channels/hls-atmospheric-correction"
CHANNEL_DIR="channel"

# No more 'pixi global install'. 
# We assume we are inside 'pixi run -e dev'

if [ -z "$SORTED_PACKAGES" ]; then
    echo "No packages to build."
    exit 0
fi

echo "--- Starting Build Sequence ---"
echo "Target: $SORTED_PACKAGES"

# 1. Sync Local Mirror
echo "⬇️ Syncing current channel from S3..."
mkdir -p "$CHANNEL_DIR/linux-64"
aws s3 sync "$CONDA_CHANNEL/linux-64" "$CHANNEL_DIR/linux-64" --delete || true

# 2. Build Loop
for pkg_dir in $SORTED_PACKAGES; do
    echo "------------------------------------------------"
    echo "🛠️  Building: $pkg_dir"
    echo "------------------------------------------------"
    
    # rattler-build is provided by the pixi dev environment
    rattler-build build \
        --recipe "packages/$pkg_dir/recipe.yml" \
        --output-dir "$CHANNEL_DIR" \
        -c "./$CHANNEL_DIR" \
        -c conda-forge \
        --experimental
        
    echo "📄 Indexing local channel..."
    rattler-index fs "$CHANNEL_DIR"
done

# 3. Upload
echo "------------------------------------------------"
echo "☁️  Uploading to S3..."
echo "------------------------------------------------"
# aws s3 sync "$CHANNEL_DIR/linux-64" "$CONDA_CHANNEL/linux-64"
rattler-build upload s3 --force --channel ${CONDA_CHANNEL} ${CHANNEL_DIR}/linux-64/*.conda
rattler-index s3 ${CONDA_CHANNEL}

echo "🎉 Done."
