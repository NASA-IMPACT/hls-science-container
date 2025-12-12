#!/bin/bash
set -e

if [ -z "$CONDA_CHANNEL" ]; then
    echo "Error: must provide Conda channel as CONDA_CHANNEL envvar"
    exit 1
fi
CHANNEL_DIR=${CHANNEL_DIR:-channel}

SORTED_PACKAGES=$1
if [ -z "$SORTED_PACKAGES" ]; then
    echo "No packages to build."
    exit 0
fi

echo "--- Starting Build Sequence ---"
echo "Target: $SORTED_PACKAGES"

# 1. Build Loop
mkdir -p "$CHANNEL_DIR/linux-64"
for pkg_dir in $SORTED_PACKAGES; do
    echo "------------------------------------------------"
    echo "🛠️  Building: $pkg_dir"
    echo "------------------------------------------------"
    rattler-build build \
        --recipe "packages/$pkg_dir/recipe.yml" \
        --output-dir "$CHANNEL_DIR" \
        -c conda-forge \
        --test native
done

# 2. Upload & reindex
echo "------------------------------------------------"
echo "☁️  Uploading to S3..."
echo "------------------------------------------------"
rattler-build upload s3 --force --channel ${CONDA_CHANNEL} ${CHANNEL_DIR}/**/*.conda
rattler-index s3 ${CONDA_CHANNEL}

echo "🎉 Done."
