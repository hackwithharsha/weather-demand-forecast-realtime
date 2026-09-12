#!/usr/bin/env sh
set -e

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# ── Buckets (idempotent) ──────────────────────────────────────────────────────
for bucket in lake mlflow exports; do
    mc mb --ignore-existing local/$bucket
done

# ── Non-root service account ──────────────────────────────────────────────────
# Create user only if it does not already exist (idempotent on volume reuse).
mc admin user info local "$MINIO_SVC_ACCESS_KEY" > /dev/null 2>&1 \
    || mc admin user add local "$MINIO_SVC_ACCESS_KEY" "$MINIO_SVC_SECRET_KEY"

# Attach the built-in readwrite policy; mc emits a warning when already
# attached so redirect stderr and use || true to stay idempotent.
mc admin policy attach local readwrite \
    --user "$MINIO_SVC_ACCESS_KEY" 2>/dev/null || true

printf 'minio-init: buckets OK, service account "%s" ready\n' \
    "$MINIO_SVC_ACCESS_KEY"
