#!/bin/bash
# Create the MLflow metadata database on first container initialisation.
# MLflow uses its own Alembic migration chain; giving it a separate database
# prevents the alembic_version table from conflicting with the application's.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "SELECT 'CREATE DATABASE mlflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec"
