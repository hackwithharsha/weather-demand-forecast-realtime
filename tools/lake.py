"""
lake CLI  —  list and preview Parquet files in the MinIO lake bucket.

Usage:
    python -m tools.lake list [PREFIX]
    python -m tools.lake preview S3_PATH [--rows N]

Environment variables:
    MINIO_ENDPOINT_URL   (default: http://minio:9000)
    MINIO_ACCESS_KEY     (default: minioadmin)
    MINIO_SECRET_KEY     (default: minioadmin)
    LAKE_BUCKET          (default: lake)
"""

from __future__ import annotations

import os
import sys

import boto3
import duckdb


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def _bucket() -> str:
    return os.environ.get("LAKE_BUCKET", "lake")


def _minio_host() -> str:
    url = os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000")
    # Strip scheme for DuckDB s3_endpoint setting
    return url.removeprefix("http://").removeprefix("https://")


def cmd_list(prefix: str = "raw/") -> None:
    s3 = _s3_client()
    bucket = _bucket()
    paginator = s3.get_paginator("list_objects_v2")
    found = False
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(obj["Key"])
            found = True
    if not found:
        print(f"(no objects found under s3://{bucket}/{prefix})")


def cmd_preview(s3_path: str, rows: int = 10) -> None:
    bucket = _bucket()
    # Accept either "raw/..." or "s3://lake/raw/..."
    if s3_path.startswith("s3://"):
        s3_path = s3_path[len(f"s3://{bucket}/"):]

    s3_url = f"s3://{bucket}/{s3_path}"
    endpoint = _minio_host()
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")

    result = con.execute(
        f"SELECT * FROM read_parquet('{s3_url}') LIMIT {rows}"
    ).fetchdf()
    print(result.to_string(index=False))


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)

    command = args[0]

    if command == "list":
        prefix = args[1] if len(args) > 1 else "raw/"
        cmd_list(prefix)

    elif command == "preview":
        if len(args) < 2:
            print("Usage: lake preview S3_PATH [--rows N]", file=sys.stderr)
            sys.exit(1)
        s3_path = args[1]
        rows = 10
        if "--rows" in args:
            idx = args.index("--rows")
            rows = int(args[idx + 1])
        cmd_preview(s3_path, rows)

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Available commands: list, preview", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
