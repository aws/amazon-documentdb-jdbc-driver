#!/bin/sh
docker build -t taco-builder .
ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
CURRENT_FOLDER="${ABSOLUTE_PATH%/*}"
mkdir -p target
docker run -d -it --name=taco-builder --mount type=bind,source="$CURRENT_FOLDER"/target,target=/output taco-builder
docker exec -ti taco-builder sh -c "cp /tableau-sdk/connector-plugin-sdk/connector-packager/packaged-connector/documentdbjdbc.taco  /output"
docker stop taco-builder
docker rm taco-builder