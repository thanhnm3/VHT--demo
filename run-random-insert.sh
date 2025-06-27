#!/bin/bash

# Build image
DOCKER_BUILDKIT=1 docker build -f docker/Dockerfile.random-insert -t random-insert .

# Run container

docker run --rm --network kafka-platform \
  -v $(pwd)/my-data-pipeline/common/src/main/resources/config-docker.yaml:/app/config-docker.yaml \
  -e CONFIG_FILE=config-docker.yaml \
  random-insert 