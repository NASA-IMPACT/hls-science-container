#!/bin/bash
set -e

CONDA_CHANNEL=${CONDA_CHANNEL:-s3://hls-conda-channels/hls-atmospheric-correction}
OUTPUT_DIR="channel-output"

SORTED_PACKAGES=$1
if [ -z "$SORTED_PACKAGES" ]; then
    echo "No packages to build."
    exit 0
fi

echo "--- Starting Build Sequence ---"
echo "Target: $SORTED_PACKAGES"

# 1. Build Loop
mkdir -p "$OUTPUT_DIR/linux-64"
for pkg_dir in $SORTED_PACKAGES; do
    echo "------------------------------------------------"
    echo "🛠️  Building: $pkg_dir"
    echo "------------------------------------------------"
    rattler-build build \
        --recipe "packages/$pkg_dir/recipe.yml" \
        --output-dir "$OUTPUT_DIR" \
        -c conda-forge \
        --test native
done

# 2. Upload & reindex
echo "------------------------------------------------"
echo "☁️  Uploading to S3..."
echo "------------------------------------------------"
rattler-build upload s3 --force --channel ${CONDA_CHANNEL} ${OUTPUT_DIR}/**/*.conda
rattler-index s3 ${CONDA_CHANNEL}

echo "🎉 Done."
