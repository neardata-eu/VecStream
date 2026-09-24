#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$PROJECT_ROOT/vecstream"
ZIP_NAME="code.zip"
DEST_DIR="$PROJECT_ROOT/deployment"

cd "$PROJECT_ROOT"

# Remove any existing zip
rm -f "$DEST_DIR/$ZIP_NAME"

# Zip the vecstream/ package as a subdirectory so Lambda can resolve
# `from vecstream.X import Y` at runtime. Include __init__.py (no longer
# excluded) so the package is a regular package.
zip -r "$DEST_DIR/$ZIP_NAME" vecstream/ \
    -x "vecstream/__pycache__/*" \
    -x "vecstream/.ipynb_checkpoints/*" \
    -x "*.pyc" \
    -x "*.pyo" \
    -x "*.egg-info/*" \
    -x "*.bak" \
    -x "*.tmp" \
    -x "*.log" \
    -x "*.out" \
    -x "*~"

echo "Packaged $ZIP_NAME in $DEST_DIR/"
