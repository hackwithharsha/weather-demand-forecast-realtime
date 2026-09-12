#!/usr/bin/env bash
# TODO: create MinIO buckets on first boot
# Run via an init container or manually after `make up-storage`.
#
# mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
# mc mb --ignore-existing local/models
# mc mb --ignore-existing local/raw
# mc mb --ignore-existing local/processed
