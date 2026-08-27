"""An object store backed by S3, or anything speaking its API.

Cloudflare R2 is the intended target (``PROJECT.md`` §11.1); MinIO and S3 itself
work unchanged, which is the point of writing against the API rather than a
vendor SDK.

The credential this is given can read and write but **not delete**
(``PROJECT.md`` §11.7). That is a deliberate pairing: the port exposes no delete
method, and the token could not perform one if it did. Losing the raw zone is
the only unrecoverable event in the system, so it is made structurally
impossible rather than merely unlikely.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from finflow.contracts.errors import ObjectAlreadyExists, ObjectNotFound

if TYPE_CHECKING:  # pragma: no cover - import cost is real, the types are not
    from collections.abc import Iterator


class S3ObjectStore:
    """Write-once blobs in a bucket."""

    def __init__(self, client: Any, bucket: str, *, prefix: str = "") -> None:
        """Take an already-configured boto3 client.

        Constructing the client here would put endpoint and credential handling
        inside the adapter, where tests cannot reach it. Wiring belongs to the
        composition root; see ``adapters.storage.s3.build_client``.
        """
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def put(self, key: str, data: bytes) -> None:
        """Write a new object, refusing to overwrite.

        S3 has no atomic create-if-absent, so this is a check followed by a
        write. The race is real but not material here: a single writer holds the
        pipeline lock, and the keys carry a microsecond timestamp, so two runs
        cannot generate the same key in practice.
        """
        if self.exists(key):
            raise ObjectAlreadyExists(key)
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=data)

    def get(self, key: str) -> bytes:
        """Read an object."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        except Exception as exc:
            if _is_missing(exc):
                raise ObjectNotFound(key) from None
            raise
        body: bytes = response["Body"].read()
        return body

    def exists(self, key: str) -> bool:
        """True when the key is present."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        except Exception as exc:
            if _is_missing(exc):
                return False
            raise
        return True

    def list(self, prefix: str = "") -> tuple[str, ...]:
        """Every key under ``prefix``, in lexicographic order.

        S3 already returns keys sorted, but the sort is repeated rather than
        assumed: the port promises ordering and a paginated listing across
        implementations is not worth trusting on that point.
        """
        return tuple(sorted(self._iter_keys(prefix)))

    def _iter_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._key(prefix)):
            for item in page.get("Contents", []):
                yield str(item["Key"])[len(self._prefix) :]


def _is_missing(exc: Exception) -> bool:
    """True when a botocore error means "no such key" rather than a real fault."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "404", "NotFound"} or status == 404


def build_client(
    *,
    endpoint_url: str | None,
    access_key_id: str,
    secret_access_key: str,
    region: str = "auto",
) -> Any:
    """Construct a boto3 S3 client pointed at R2, MinIO or S3.

    Imported lazily so that the daily path does not pay boto3's import cost when
    the local store is in use, which is the common case in development.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )
