#!/bin/bash

# Build the image first
docker build -f docker/Dockerfile -t data-pipeline:latest .

# Run the container in 'cdc' mode
docker run --rm \
  --name pipeline-cdc \
  --network kafka-platform \
  -e JAVA_OPTS="-Xms1g -Xmx3g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication" \
  -e CONFIG_FILE=config-docker.yaml \
  -e MODE=cdc \
  -v $(pwd)/my-data-pipeline/common/src/main/resources/config-docker.yaml:/app/config-docker.yaml \
  --cpus=2.0 \
  --memory=4g \
  data-pipeline:latest