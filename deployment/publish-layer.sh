#!/bin/bash

LAYER_NAME="vecstream-layer.zip"

cd "$(dirname "$0")"

aws lambda publish-layer-version \
    --layer-name vecstream \
    --zip-file fileb://vecstream-layer.zip \
    --compatible-runtimes python3.13
