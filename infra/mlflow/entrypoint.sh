#!/bin/sh
# Start MLflow tracking server.
# MLFLOW_BACKEND_STORE_URI and MLFLOW_DEFAULT_ARTIFACT_ROOT are injected
# by docker-compose.yml; AWS_* env vars configure the MinIO artifact store.
#
# --additional-allowed-hosts mlflow: the Docker service hostname ('mlflow')
# is rejected by MLflow's DNS-rebinding protection by default; this flag
# adds it to the allowed list so intra-container requests succeed.
set -e
exec mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri  "${MLFLOW_BACKEND_STORE_URI}" \
    --default-artifact-root "${MLFLOW_DEFAULT_ARTIFACT_ROOT}" \
    --serve-artifacts \
    --allowed-hosts '*'
