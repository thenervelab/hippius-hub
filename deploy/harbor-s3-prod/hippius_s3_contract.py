#!/usr/bin/env python3
"""Harbor-shaped S3 contract against hippius-s3 (not MinIO, not JuiceFS).

Exercises the distribution S3-driver ops Harbor will use after the flip:
single PUT (tiny + 64 MiB pack), MPU, CopyObject (blob commit Move),
HEAD/GET/Range, List prefix, Delete. Path-style SigV4 over HTTP.

Requires HARBOR_S3_BUCKET + access key. Writes only under
harbor-s3-probe/<run-id>/ and deletes that prefix on exit.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any

PROBE_PREFIX = "harbor-s3-probe"
TINY_SIZE = 256
PACK_SIZE = 64 * 1024 * 1024
MPU_PART = 8 * 1024 * 1024
DEFAULT_ENDPOINT = "http://gateway.hippius-s3-prod.svc.cluster.local:8080"
FORBIDDEN_BUCKETS = frozenset({"hippius-juicefs-data"})
# Streaming CopyObject (pre-#445) was 4.3 MiB/s. Alias CopyObject was 657.
# 50 MiB/s is the discriminator: below this, Harbor Move is still GET+PUT.
COPY_OBJECT_ALIAS_MIN_MIBS = 50.0


def copy_object_too_slow(mibs: float) -> str | None:
    if mibs < COPY_OBJECT_ALIAS_MIN_MIBS:
        return (
            f"CopyObject 64 MiB {mibs:.1f} MiB/s is streaming GET+PUT, not an "
            f"object_names alias (bar {COPY_OBJECT_ALIAS_MIN_MIBS:.0f} MiB/s). "
            "Promote hippius-s3 #445/#448/#451 to this gateway before Harbor S3."
        )
    return None


def bucket_forbidden(name: str) -> str | None:
    lowered = name.strip().lower()
    if lowered in FORBIDDEN_BUCKETS:
        return f"bucket {name!r} is the JuiceFS backend — refuse"
    if "juicefs" in lowered:
        return f"bucket {name!r} looks like JuiceFS — refuse"
    return None


def endpoint_forbidden(url: str) -> str | None:
    lowered = url.lower()
    if "minio" in lowered:
        return f"endpoint {url!r} is MinIO — this gate is hippius-s3"
    if "hippius-s3" not in lowered and "s3.hippius.com" not in lowered:
        return f"endpoint {url!r} is not hippius-s3"
    return None


def env_config() -> dict[str, str]:
    access = os.environ.get("HARBOR_S3_ACCESS_KEY") or os.environ.get(
        "AWS_ACCESS_KEY_ID", ""
    )
    secret = os.environ.get("HARBOR_S3_SECRET_KEY") or os.environ.get(
        "AWS_SECRET_ACCESS_KEY", ""
    )
    bucket = os.environ.get("HARBOR_S3_BUCKET", "")
    endpoint = os.environ.get("HARBOR_S3_ENDPOINT", DEFAULT_ENDPOINT)
    if not access or not secret or not bucket:
        sys.exit(
            "need HARBOR_S3_BUCKET and HARBOR_S3_ACCESS_KEY / HARBOR_S3_SECRET_KEY"
        )
    err = bucket_forbidden(bucket) or endpoint_forbidden(endpoint)
    if err:
        sys.exit(err)
    return {
        "access": access,
        "secret": secret,
        "bucket": bucket,
        "endpoint": endpoint.rstrip("/"),
    }


def s3_client(cfg: dict[str, str]) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=cfg["endpoint"],
        aws_access_key_id=cfg["access"],
        aws_secret_access_key=cfg["secret"],
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            retries={"max_attempts": 8, "mode": "standard"},
            connect_timeout=10,
            read_timeout=300,
        ),
    )


def error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None) or {}
    return str(response.get("Error", {}).get("Code", ""))


def retrying(fn: Callable[[], Any], *, tries: int = 12, retry_404: bool = False) -> Any:
    from botocore.exceptions import ClientError

    delay = 1.0
    last: Exception | None = None
    for _ in range(tries):
        try:
            return fn()
        except ClientError as exc:
            last = exc
            code = error_code(exc)
            if code in {"InsufficientAccountCredit", "402"}:
                raise SystemExit(
                    "FAIL  credits: can_upload 402 InsufficientAccountCredit"
                ) from exc
            http = int(
                exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            )
            if code in {"SlowDown", "ServiceUnavailable"} or http in {503, 429}:
                time.sleep(delay)
                delay = min(delay * 1.5, 8.0)
                continue
            if retry_404 and (code in {"404", "NoSuchKey", "NotFound"} or http == 404):
                time.sleep(delay)
                delay = min(delay * 1.5, 8.0)
                continue
            raise
    assert last is not None
    raise last


def blob_key(prefix: str, digest: str) -> str:
    return f"{prefix}/docker/registry/v2/blobs/sha256/{digest[:2]}/{digest}/data"


def upload_key(prefix: str, name: str) -> str:
    return f"{prefix}/docker/registry/v2/repositories/probe/_uploads/{name}/data"


def put_and_read(client: Any, bucket: str, key: str, body: bytes) -> float:
    t0 = time.perf_counter()
    retrying(lambda: client.put_object(Bucket=bucket, Key=key, Body=body))
    dt = time.perf_counter() - t0
    head = retrying(
        lambda: client.head_object(Bucket=bucket, Key=key),
        retry_404=True,
    )
    if int(head["ContentLength"]) != len(body):
        raise SystemExit(f"FAIL  HEAD size {head['ContentLength']} != {len(body)}")
    got = retrying(
        lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read(),
        retry_404=True,
    )
    if got != body:
        raise SystemExit(f"FAIL  GET bytes mismatch for {key}")
    return dt


def range_get(client: Any, bucket: str, key: str, body: bytes) -> None:
    first = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-1023")
    chunk = first["Body"].read()
    if chunk != body[:1024]:
        raise SystemExit("FAIL  Range GET prefix mismatch")
    last = client.get_object(
        Bucket=bucket,
        Key=key,
        Range=f"bytes={len(body) - 1024}-{len(body) - 1}",
    )
    if last["Body"].read() != body[-1024:]:
        raise SystemExit("FAIL  Range GET suffix mismatch")


def mpu(client: Any, bucket: str, key: str) -> bytes:
    parts = [os.urandom(MPU_PART), os.urandom(MPU_PART)]
    body = b"".join(parts)
    uid = retrying(
        lambda: client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    )
    completed: list[dict[str, Any]] = []
    for i, part in enumerate(parts, start=1):
        resp = retrying(
            lambda p=part, n=i: client.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=n,
                UploadId=uid,
                Body=p,
            )
        )
        completed.append({"ETag": resp["ETag"], "PartNumber": i})
    retrying(
        lambda: client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=uid,
            MultipartUpload={"Parts": completed},
        )
    )
    got = retrying(
        lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read(),
        retry_404=True,
    )
    if got != body:
        raise SystemExit("FAIL  MPU GET bytes mismatch")
    return body


def copy_object(client: Any, bucket: str, src: str, dst: str, expect: bytes) -> float:
    t0 = time.perf_counter()
    retrying(
        lambda: client.copy_object(
            Bucket=bucket,
            Key=dst,
            CopySource={"Bucket": bucket, "Key": src},
        )
    )
    dt = time.perf_counter() - t0
    got = retrying(
        lambda: client.get_object(Bucket=bucket, Key=dst)["Body"].read(),
        retry_404=True,
    )
    if got != expect:
        raise SystemExit(f"FAIL  CopyObject bytes mismatch {src} -> {dst}")
    return dt


def list_has(client: Any, bucket: str, prefix: str, key: str) -> None:
    found = False
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token
        page = retrying(lambda k=kwargs: client.list_objects_v2(**k))
        for obj in page.get("Contents") or []:
            if obj["Key"] == key:
                found = True
                break
        if found or not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    if not found:
        raise SystemExit(f"FAIL  ListObjectsV2 missed {key}")


def delete_prefix(client: Any, bucket: str, prefix: str) -> None:
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents") or []]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        if not page.get("IsTruncated"):
            return
        token = page.get("NextContinuationToken")


def pass_(name: str, detail: str = "") -> None:
    extra = f"  {detail}" if detail else ""
    print(f"PASS  {name}{extra}", flush=True)


def main() -> None:
    cfg = env_config()
    run_id = uuid.uuid4().hex[:12]
    prefix = f"{PROBE_PREFIX}/{run_id}"
    client = s3_client(cfg)
    bucket = cfg["bucket"]
    print(f"endpoint {cfg['endpoint']}", flush=True)
    print(f"bucket   {bucket}", flush=True)
    print(f"prefix   {prefix}/", flush=True)

    retrying(lambda: client.head_bucket(Bucket=bucket))
    pass_("head_bucket")

    try:
        tiny = os.urandom(TINY_SIZE)
        tiny_src = upload_key(prefix, "tiny")
        tiny_dt = put_and_read(client, bucket, tiny_src, tiny)
        pass_("put_head_get_tiny", f"{tiny_dt:.2f}s")

        tiny_dst = blob_key(prefix, hashlib.sha256(tiny).hexdigest())
        copy_object(client, bucket, tiny_src, tiny_dst, tiny)
        pass_("copy_object_tiny")

        pack = os.urandom(PACK_SIZE)
        digest = hashlib.sha256(pack).hexdigest()
        pack_src = upload_key(prefix, "pack64")
        pack_dt = put_and_read(client, bucket, pack_src, pack)
        mibs = PACK_SIZE / pack_dt / (1024 * 1024)
        pass_("put_head_get_64mib", f"{pack_dt:.2f}s {mibs:.1f} MiB/s")
        range_get(client, bucket, pack_src, pack)
        pass_("range_get_64mib")

        pack_dst = blob_key(prefix, digest)
        copy_dt = copy_object(client, bucket, pack_src, pack_dst, pack)
        copy_mibs = PACK_SIZE / copy_dt / (1024 * 1024)
        slow = copy_object_too_slow(copy_mibs)
        if slow:
            raise SystemExit(f"FAIL  {slow}")
        pass_("copy_object_64mib", f"{copy_dt:.2f}s {copy_mibs:.1f} MiB/s")

        mpu_src = upload_key(prefix, "mpu")
        mpu_body = mpu(client, bucket, mpu_src)
        pass_("mpu_16mib_two_parts")
        mpu_dst = blob_key(prefix, hashlib.sha256(mpu_body).hexdigest())
        copy_object(client, bucket, mpu_src, mpu_dst, mpu_body)
        pass_("copy_object_mpu")

        list_has(client, bucket, f"{prefix}/docker/registry/v2/blobs/", pack_dst)
        pass_("list_blobs_prefix")

        retrying(lambda: client.delete_object(Bucket=bucket, Key=pack_src))
        pass_("delete_object")
    finally:
        try:
            delete_prefix(client, bucket, prefix + "/")
            print(f"cleaned {prefix}/", flush=True)
        except Exception as exc:
            print(f"cleanup warning: {exc}", flush=True)

    print("PASS  hippius-s3 Harbor contract", flush=True)


if __name__ == "__main__":
    main()
