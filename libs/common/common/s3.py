"""
S3-compatible (MinIO) client factory.

Both functions accept any object that satisfies the ``_S3Config`` Protocol —
in practice, a service's ``Settings`` instance.

Usage
-----
    from common.s3 import make_s3_client, make_s3fs
    from .settings import Settings

    settings = Settings()

    # Raw boto3 client — put_object, get_object, list_objects_v2, …
    s3 = make_s3_client(settings)
    s3.put_object(Bucket="lake", Key="raw/…/data.parquet", Body=buf)

    # s3fs filesystem — pandas / PyArrow read_parquet, open(), glob(), …
    fs = make_s3fs(settings)
    df = pd.read_parquet("s3://lake/raw/…/data.parquet", filesystem=fs)
"""

from __future__ import annotations

from typing import Any, Protocol

import boto3
import s3fs


class _S3Config(Protocol):
    """Structural type expected by both factory functions.

    Any ``Settings`` class that carries these three attributes satisfies this
    protocol without inheriting from it.
    """

    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str


def make_s3_client(cfg: _S3Config) -> Any:
    """Return a boto3 S3 client pre-configured for the internal MinIO endpoint.

    ``signature_version='s3v4'`` is required by MinIO for all requests.
    Path-style addressing is the default for non-AWS endpoints.
    """
    return boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint_url,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def make_s3fs(cfg: _S3Config) -> s3fs.S3FileSystem:
    """Return an s3fs filesystem pre-configured for the internal MinIO endpoint.

    Disables SSL for plain ``http://`` endpoints; enables it for ``https://``.
    Use this filesystem with pandas ``read_parquet`` / ``to_parquet`` and
    PyArrow's ``dataset`` / ``parquet.read_table`` via the ``filesystem``
    argument.
    """
    use_ssl = cfg.minio_endpoint_url.startswith("https://")
    return s3fs.S3FileSystem(
        key=cfg.minio_access_key,
        secret=cfg.minio_secret_key,
        endpoint_url=cfg.minio_endpoint_url,
        use_ssl=use_ssl,
    )
