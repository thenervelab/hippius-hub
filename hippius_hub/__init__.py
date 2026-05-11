"""hippius_hub — drop-in replacement for huggingface_hub against an OCI registry.

Phase A: hf_hub_download / snapshot_download / try_to_load_from_cache /
hf_hub_url / login / logout / whoami + HF error and dataclass re-exports.
Phase B: upload_file / upload_folder / create_repo / delete_repo /
repo_info / model_info / list_repo_files / repo_exists / revision_exists /
file_exists, plus HippiusApi(HfApi) subclass with auto-stubbed unsupported
methods.
"""

from . import errors

from .file_download import (
    hf_hub_download,
    hf_hub_url,
    hippius_hub_download,
    try_to_load_from_cache,
)
from .file_upload import hippius_hub_upload, upload_file, upload_folder
from ._snapshot_download import snapshot_download
from ._repo_ops import (
    create_repo,
    delete_repo,
    file_exists,
    list_repo_files,
    model_info,
    repo_exists,
    repo_info,
    revision_exists,
)
from .auth import get_token, login, logout, whoami
from .hf_api import HippiusApi

# Re-export huggingface_hub typed exceptions
from .errors import (
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

# Re-export huggingface_hub dataclasses so consumers receive HF-compatible types
from huggingface_hub import (
    CardData,
    CommitInfo,
    CommitOperationAdd,
    CommitOperationDelete,
    DatasetCard,
    DatasetInfo,
    GitRefInfo,
    GitRefs,
    HfFileMetadata,
    ModelCard,
    ModelCardData,
    ModelInfo,
    RepoCard,
    RepoFile,
    RepoFolder,
    RepoUrl,
    SpaceInfo,
)
from huggingface_hub.file_download import _CACHED_NO_EXIST
from huggingface_hub.hf_api import RepoSibling


__all__ = [
    # Phase A public API
    "hf_hub_download",
    "hippius_hub_download",
    "hippius_hub_upload",
    "snapshot_download",
    "try_to_load_from_cache",
    "hf_hub_url",
    "login",
    "logout",
    "get_token",
    "whoami",
    "HippiusApi",
    "errors",
    # Phase B authoring + inspection
    "upload_file",
    "upload_folder",
    "create_repo",
    "delete_repo",
    "repo_info",
    "model_info",
    "list_repo_files",
    "repo_exists",
    "revision_exists",
    "file_exists",
    # Errors
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
    # Dataclass re-exports
    "_CACHED_NO_EXIST",
    "CardData",
    "CommitInfo",
    "CommitOperationAdd",
    "CommitOperationDelete",
    "DatasetCard",
    "DatasetInfo",
    "GitRefInfo",
    "GitRefs",
    "HfFileMetadata",
    "ModelCard",
    "ModelCardData",
    "ModelInfo",
    "RepoCard",
    "RepoFile",
    "RepoFolder",
    "RepoSibling",
    "RepoUrl",
    "SpaceInfo",
]
