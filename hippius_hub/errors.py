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


class ManifestTooLargeError(ValueError):
    """The assembled manifest exceeds the registry's maximum manifest body size.

    CNCF Distribution caps a manifest PUT at 4 MiB (`maxManifestBodySize`). An
    artifact with enough chunk layers to blow that budget (tens of thousands — a
    >1 TB single file at the 64 MiB chunk average) would be rejected by the
    registry with an opaque 400 *after* every blob is already uploaded. We check
    the serialized size before the PUT and raise this instead. The fix for
    genuinely huge artifacts is Referrers/index fan-out (a documented follow-up).
    `ValueError` so broad `except ValueError`/`except Exception` handlers catch it.
    """


class MalformedManifestError(ValueError):
    """A manifest declares a known layout but violates its structural contract.

    Distinct from `UnsupportedLayoutError` (an *unknown* layout we refuse on
    principle): here the layout is `chunked-v1` — one we claim to read — but the
    bytes don't hold up (a pointer layer missing its whole-file size/digest, a
    non-integer chunk count, or fewer trailing chunk layers than the count
    promises). Reassembling from it would silently produce a truncated or wrong
    file, so we stop. `ValueError` so callers using broad `except ValueError` /
    `except Exception` still catch it.
    """


class UnsupportedLayoutError(RuntimeError):
    """The manifest declares a `com.hippius.layout` this build cannot read.

    Raised at manifest-fetch time when the annotation is present with a value
    outside `constants.KNOWN_LAYOUTS` — a newer client wrote an artifact layout
    (e.g. a future `chunked-vN`) this build predates. Reading it as an ordinary
    manifest would misresolve files (a chunked file's pointer layer would be
    written verbatim as the file), so we fail loudly with an upgrade hint
    instead of silently corrupting the download.

    `RuntimeError`, not `HfHubHTTPError`: the condition is a client-capability
    gap discovered while parsing a successfully-fetched manifest, not an HTTP
    status — there is no meaningful `response` to attach.
    """


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


class ManifestBlobUnknownError(HfHubHTTPError):
    """A manifest PUT was rejected because the registry can't resolve a blob it
    references — Harbor's ``MANIFEST_BLOB_UNKNOWN`` — *after* the manifest-PUT
    commit-visibility retry budget is already spent.

    This is the durable case, distinct from the transient commit→visibility lag
    the manifest retry rides out: reaching here means the referenced blob is
    genuinely absent (a registry-side GC reaped an untagged blob, or a commit
    that returned 2xx never landed under storage pressure), so the content must
    be re-pushed, not merely re-awaited. ``upload_file`` catches this to re-run
    the upload (re-PUTting every referenced blob) a bounded number of times
    before surfacing it.

    Subclasses ``HfHubHTTPError`` so ``except HfHubHTTPError:`` handlers still
    catch it. ``missing_digests`` carries the blob digests Harbor named in the
    error body (a best-effort scan — a stripping proxy can mangle the JSON), for
    logging and future targeted re-push.
    """

    def __init__(self, message: str, *, response: httpx.Response, missing_digests: tuple = ()):
        super().__init__(message, response=response)
        self.missing_digests = tuple(missing_digests)


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
    "ManifestBlobUnknownError",
    "ManifestTooLargeError",
    "MalformedManifestError",
    "OfflineModeIsEnabled",
    "RepositoryNotFoundError",
    "RevisionNotFoundError",
    "UnsupportedLayoutError",
]
