#!/bin/bash
set -e

export IMAGE_NAME=angel:artifacts

docker build --target ARTIFACTS -t ${IMAGE_NAME} .
docker run -it --rm -v $(pwd)/dist:/output ${IMAGE_NAME}
echo "***** output package in ./dist *****"