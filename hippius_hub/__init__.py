"""hippius_hub — drop-in replacement for huggingface_hub against an OCI registry.

Phase A: hf_hub_download / snapshot_download / try_to_load_from_cache /
hf_hub_url / login / logout / whoami + HF error and dataclass re-exports.
Phase B: upload_file / upload_folder / create_repo / delete_repo /
repo_info / model_info / list_repo_files / repo_exists / revision_exists /
file_exists, plus HippiusApi(HfApi) subclass with auto-stubbed unsupported
methods.
"""

from . import errors

# Resolve the installed package version. Falls back to "0.0.0+unknown" when
# running from a source tree without metadata (e.g. `pytest` against an
# in-tree checkout that wasn't `pip install -e`'d). Either way, no import
# error.
try:
    from importlib.metadata import version as _pkg_version, PackageNotFoundError
    try:
        __version__ = _pkg_version("hippius_hub")
    except PackageNotFoundError:
        __version__ = "0.0.0+unknown"
except ImportError:  # Python < 3.8 — not supported, but be defensive
    __version__ = "0.0.0+unknown"

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
    list_repo_refs,
    model_info,
    repo_exists,
    repo_info,
    revision_exists,
)
from .auth import get_token, login, logout, whoami
# Card classes are subclasses defined in `.hf_api` (NOT bare re-exports from
# huggingface_hub) — the HF originals have `push_to_hub`/`load` methods that
# hit huggingface.co. Importing them from this package gives users the data
# shape but raises if they call the network methods, instead of silently
# routing README I/O at HF.
from .hf_api import DatasetCard, HippiusApi, ModelCard, RepoCard

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

# Re-export huggingface_hub dataclasses so consumers receive HF-compatible types.
# RepoCard / ModelCard / DatasetCard are NOT in this list — they're imported
# above from `.hf_api` instead, as Hippius-safe subclasses that block the HF
# network methods.
from huggingface_hub import (
    CardData,
    CommitInfo,
    CommitOperationAdd,
    CommitOperationDelete,
    DatasetInfo,
    GitRefInfo,
    GitRefs,
    HfFileMetadata,
    ModelCardData,
    ModelInfo,
    RepoFile,
    RepoFolder,
    RepoUrl,
    SpaceInfo,
)
from huggingface_hub.file_download import _CACHED_NO_EXIST
from huggingface_hub.hf_api import RepoSibling


__all__ = [
    "__version__",
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
    "list_repo_refs",
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
