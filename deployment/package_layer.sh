#!/bin/bash
set -e

LAYER_NAME="vecstream-layer.zip"
BUILD_DIR="python"
DEST_DIR="."

cd "$(dirname "$0")"

# Remove any existing build dir and zip
rm -rf "$BUILD_DIR" "$LAYER_NAME"
mkdir "$BUILD_DIR"

# Install vecstream dependencies into the Lambda layer layout.
# Version constraints match pyproject.toml. boto3 is excluded because the
# AWS Lambda Python 3.13 runtime already provides it.
uv pip install \
    --python 3.13 \
    --target "$BUILD_DIR/" \
    "numpy>=2.4.2" \
    "faiss-cpu>=1.13.2" \
    "aiohttp>=3.13.3" \
    "confluent-kafka>=2.14.2" \
    "mmh3>=5.2.1" \
    "lz4>=4.4.5" \
    "orjson>=3.11.9"

# Zip the python/ tree into a Lambda layer archive
zip -r "$LAYER_NAME" "$BUILD_DIR/" \
    -x "$BUILD_DIR/__pycache__/*" \
    -x "*.pyc" \
    -x "*.egg-info/*" \
    -x "*.dist-info/*"

rm -rf "$BUILD_DIR"
echo "Packaged $LAYER_NAME in $DEST_DIR/"
