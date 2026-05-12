import os
from typing import Optional

DEFAULT_CACHE_DIR = os.path.expanduser("~/.cache/hippius/hub")
DEFAULT_REGISTRY_URL = "https://registry.hippius.com"


def resolve_registry(endpoint: Optional[str]) -> str:
    """Return the user-provided endpoint (trailing slash trimmed) or the default."""
    return (endpoint or DEFAULT_REGISTRY_URL).rstrip("/")

# httpx timeout for control-plane calls (manifest fetch, blob HEAD, Harbor admin
# API). Long enough to absorb ATS proxy stalls; not used by the Rust blob fetcher.
DEFAULT_HTTP_TIMEOUT = 30.0

# OCI media-type Accept header for manifest reads (v1 and Docker fallback).
OCI_MANIFEST_ACCEPT = (
    "application/vnd.oci.image.manifest.v1+json, "
    "application/vnd.docker.distribution.manifest.v2+json"
)

# OCI manifest annotation that carries the in-repo filename for a layer.
LAYER_TITLE_KEY = "org.opencontainers.image.title"
