#!/usr/bin/env sh
set -e
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
for bucket in lake mlflow exports; do
    mc mb --ignore-existing local/$bucket
done
