"""Guards for the hippius-s3 Harbor contract (no live S3)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_CONTRACT = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "harbor-s3-prod"
    / "hippius_s3_contract.py"
)


def _mod():
    spec = importlib.util.spec_from_file_location("hippius_s3_contract", _CONTRACT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_refuses_juicefs_bucket():
    mod = _mod()
    assert mod.bucket_forbidden("hippius-juicefs-data")
    assert mod.bucket_forbidden("harbor-juicefs-scratch")
    assert mod.bucket_forbidden("harbor-registry-cas") is None


def test_refuses_minio_endpoint():
    mod = _mod()
    assert mod.endpoint_forbidden("http://minio.harbor-staging.svc.cluster.local:9000")
    assert (
        mod.endpoint_forbidden("http://gateway.hippius-s3-prod.svc.cluster.local:8080")
        is None
    )
    assert mod.endpoint_forbidden("https://s3.hippius.com") is None
    assert mod.endpoint_forbidden("http://gateway.example.com:8080")


def test_harbor_blob_key_layout():
    mod = _mod()
    digest = "a" * 64
    key = mod.blob_key("harbor-s3-probe/run1", digest)
    assert key == (
        f"harbor-s3-probe/run1/docker/registry/v2/blobs/sha256/{digest[:2]}"
        f"/{digest}/data"
    )


def test_env_config_refuses_juicefs_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _mod()
    monkeypatch.setenv("HARBOR_S3_BUCKET", "hippius-juicefs-data")
    monkeypatch.setenv("HARBOR_S3_ACCESS_KEY", "hip_x")
    monkeypatch.setenv("HARBOR_S3_SECRET_KEY", "y")
    with pytest.raises(SystemExit, match="JuiceFS"):
        mod.env_config()
