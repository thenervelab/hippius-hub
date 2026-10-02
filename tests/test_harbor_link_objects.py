"""Key derivation and guards for the Harbor link-object rebuild (no live S3/DB).

The keys these build are the difference between an artifact resolving and a
404 after the storage-driver flip, so the shapes are pinned against the layout
verified on the live registry filesystem (2026-08-27).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

_DEPLOY = Path(__file__).resolve().parents[1] / "deploy" / "harbor-s3-prod"

HEX = "0123456789abcdef"
DIGESTS = st.text(alphabet=HEX, min_size=64, max_size=64).map(lambda h: f"sha256:{h}")
REPOS = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Nd"), whitelist_characters="-/_"
    ),
    min_size=1,
    max_size=40,
).filter(lambda s: "//" not in s and not s.startswith("/") and not s.endswith("/"))


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _DEPLOY / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def links():
    return _load("harbor_link_objects")


# --- key shape ------------------------------------------------------------


def test_layer_key_matches_the_registry_layout(links):
    digest = "sha256:" + "f8" * 32
    key = links.link_key("0x998/albedo-qwen3.6-35b-miner", digest, manifest=False)

    assert key == (
        "docker/registry/v2/repositories/0x998/albedo-qwen3.6-35b-miner"
        f"/_layers/sha256/{'f8' * 32}/link"
    )


def test_manifest_key_goes_to_revisions_not_layers(links):
    digest = "sha256:" + "0f" * 32
    key = links.link_key("cascade/ckpt-u56", digest, manifest=True)

    assert "/_manifests/revisions/sha256/" in key
    assert "/_layers/" not in key


def test_key_omits_rootdirectory(links):
    """`rootdirectory` is unset in the overlay, so keys start at `docker/`."""
    key = links.link_key("library/swe-test", "sha256:" + "ab" * 32, manifest=False)

    assert key.startswith("docker/registry/v2/repositories/")
    assert not key.startswith("/")


def test_body_is_the_bare_digest_at_71_bytes(links):
    """The registry stores exactly `sha256:<hex>` — a trailing newline breaks it."""
    digest = "sha256:" + "9a" * 32
    body = links.link_body(digest)

    assert body == digest.encode()
    assert len(body) == links.LINK_SIZE == 71
    assert not body.endswith(b"\n")


@pytest.mark.parametrize(
    "bad",
    [
        "sha256:" + "F8" * 32,  # uppercase hex
        "sha256:" + "ab" * 31,  # short
        "sha512:" + "ab" * 32,  # wrong algorithm
        "ab" * 32,  # no algorithm prefix
        "sha256:../../etc/passwd",
        "",
    ],
)
def test_malformed_digests_are_refused(links, bad):
    with pytest.raises(ValueError):
        links.link_key("library/swe-test", bad, manifest=False)


@given(repository=REPOS, digest=DIGESTS, manifest=st.booleans())
def test_key_is_well_formed_for_any_valid_input(repository, digest, manifest):
    links = _load("harbor_link_objects")
    key = links.link_key(repository, digest, manifest=manifest)

    assert key.endswith("/link")
    assert ".." not in key
    assert digest.split(":", 1)[1] in key
    assert len(links.link_body(digest)) == links.LINK_SIZE


# --- guards, and drift between the two copies -----------------------------


@pytest.mark.parametrize("module", ["harbor_link_objects", "hippius_s3_contract"])
@pytest.mark.parametrize(
    ("bucket", "refused"),
    [
        ("hippius-juicefs-data", True),
        ("harbor-juicefs-scratch", True),
        ("HIPPIUS-JUICEFS-DATA", True),
        ("harbor-registry-cas", False),
    ],
)
def test_juicefs_buckets_are_refused_by_both_scripts(module, bucket, refused):
    assert bool(_load(module).bucket_forbidden(bucket)) is refused


@pytest.mark.parametrize("module", ["harbor_link_objects", "hippius_s3_contract"])
@pytest.mark.parametrize(
    ("endpoint", "refused"),
    [
        ("http://minio.harbor-staging.svc.cluster.local:9000", True),
        ("http://gateway.example.svc.cluster.local:8080", True),
        ("http://gateway.hippius-s3-prod.svc.cluster.local:8080", False),
        ("https://s3.hippius.com", False),
    ],
)
def test_endpoints_are_gated_by_both_scripts(module, endpoint, refused):
    assert bool(_load(module).endpoint_forbidden(endpoint)) is refused


def test_the_two_guard_copies_have_not_drifted():
    """Both scripts can write to the bucket, so neither may relax on its own."""
    links = _load("harbor_link_objects")
    contract = _load("hippius_s3_contract")

    assert links.FORBIDDEN_BUCKETS == contract.FORBIDDEN_BUCKETS

    for probe in (
        "hippius-juicefs-data",
        "x-juicefs",
        "harbor-registry-cas",
        "hub-test",
    ):
        assert bool(links.bucket_forbidden(probe)) == bool(
            contract.bucket_forbidden(probe)
        )

    for probe in ("http://minio:9000", "https://s3.hippius.com", "http://elsewhere"):
        assert bool(links.endpoint_forbidden(probe)) == bool(
            contract.endpoint_forbidden(probe)
        )


# --- the SQL that encodes the derivation rule -----------------------------


def test_layer_query_excludes_manifest_digests(links):
    """A manifest digest appears in artifact_blob but must never become a _layers link.

    Verified live: for cascade/ckpt-…-u56 the database lists 6 blob digests and
    the filesystem holds 5 `_layers` links plus 1 revision link — the missing
    one being exactly the artifact's own digest.
    """
    sql = " ".join(links.LAYER_LINK_SQL.split()).lower()

    assert "not exists" in sql
    assert "m.repository_name = a.repository_name" in sql
    assert "m.digest = ab.digest_blob" in sql


def test_revision_query_covers_every_artifact(links):
    sql = " ".join(links.REVISION_LINK_SQL.split()).lower()

    assert sql == "select distinct repository_name, digest from artifact"
