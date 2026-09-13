FROM ghcr.io/mlflow/mlflow:latest

# psycopg2-binary: SQLAlchemy backend for Postgres metadata store.
# boto3: artifact I/O against the MinIO S3-compatible object store.
RUN pip install --no-cache-dir psycopg2-binary boto3

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5000

# MLFLOW_BACKEND_STORE_URI, MLFLOW_DEFAULT_ARTIFACT_ROOT, AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, and MLFLOW_S3_ENDPOINT_URL are injected at runtime
# by docker-compose.yml.
ENTRYPOINT ["/entrypoint.sh"]
