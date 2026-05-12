"""Re-exports of huggingface_hub.errors so user code catching HF's typed
exceptions keeps working when imports are switched to hippius_hub.
"""
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


__all__ = [
    "BadRequestError",
    "CacheNotFound",
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
