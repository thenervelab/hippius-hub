"""Re-exports of huggingface_hub.errors so user code catching HF's typed
exceptions keeps working when imports are switched to hippius_hub.

Hippius-specific exceptions are also defined here. They subclass the closest
HF analog so callers writing `except HfHubHTTPError:` keep catching them.
"""
from typing import Optional

import httpx

from huggingface_hub.errors import (
    BadRequestError,
    CacheNotFound,
    CorruptedCacheException,
    DisabledRepoError,
    EntryNotFoundError,
    GatedRepoError,
    HfHubHTTPError,
    LocalEntryNotFoundError,
    LocalTokenNotFoundError,
    OfflineModeIsEnabled,
    RepositoryNotFoundError,
    RevisionNotFoundError,
)


class ConcurrentManifestUpdateError(HfHubHTTPError):
    """Manifest at this revision changed between our read and our write.

    Raised when the OCI registry returns 412 Precondition Failed from a
    PUT with If-Match. Indicates that another writer pushed a manifest to
    the same `repo_id:revision` after we fetched the current digest, so
    blindly overwriting would silently drop that writer's layer.

    The caller's options are:

    - retry the upload (re-fetch, re-merge, re-PUT — risks livelock under
      sustained contention)
    - serialize uploads to the same revision externally

    Subclasses `HfHubHTTPError` so existing `except HfHubHTTPError:` handlers
    catch it without changes. The `response` kwarg is optional because the
    raise site is the only construction point and the 412 response is the
    only meaningful payload — pass it through if available.
    """

    def __init__(self, message: str, *, response: Optional[httpx.Response] = None):
        # HfHubHTTPError's __init__ insists on a non-None response (it reads
        # headers off it). Synthesize a minimal 412 response when the caller
        # didn't have one to hand — keeps the public ctor ergonomic while
        # preserving HF's invariant that `.response` is always present.
        if response is None:
            response = httpx.Response(status_code=412, request=httpx.Request("PUT", "about:blank"))
        super().__init__(message, response=response)


__all__ = [
    "BadRequestError",
    "CacheNotFound",
    "ConcurrentManifestUpdateError",
    "CorruptedCacheException",
    "DisabledRepoError",
    "EntryNotFoundError",
    "GatedRepoError",
    "HfHubHTTPError",
    "LocalEntryNotFoundError",
    "LocalTokenNotFoundError",
    "OfflineModeIsEnabled",
    "RepositoryNotFoundError",
    "RevisionNotFoundError",
]
