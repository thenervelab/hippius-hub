"""HippiusApi: HfApi subclass routing supported methods to hippius_hub's
OCI backend. Every HfApi method we don't implement is auto-overridden at
class-construction time to raise NotImplementedError, so users discover
unsupported HF-only features the first time they call them rather than
having those calls silently hit huggingface.co.
"""
from typing import Optional, Union

from huggingface_hub import HfApi, ModelCard

from ._repo_ops import (
    create_repo as _create_repo,
    delete_repo as _delete_repo,
    file_exists as _file_exists,
    list_repo_files as _list_repo_files,
    model_info as _model_info,
    repo_exists as _repo_exists,
    repo_info as _repo_info,
    revision_exists as _revision_exists,
)
from ._snapshot_download import snapshot_download as _snapshot_download
from .auth import (
    login as _login_module,
    logout as _logout_module,
    whoami as _whoami_module,
)
from .file_download import hf_hub_download as _hf_hub_download
from .file_upload import upload_file as _upload_file, upload_folder as _upload_folder


class HippiusApi(HfApi):
    """HfApi-compatible client backed by registry.hippius.com (Harbor + OCI).

    Supported methods (Phase A+B) delegate to hippius_hub's module-level
    functions. Every other inherited HfApi method raises NotImplementedError
    — see the auto-stub registration below the class body.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        token: Union[str, bool, None] = None,
        library_name: Optional[str] = None,
        library_version: Optional[str] = None,
        user_agent=None,
        headers=None,
    ):
        super().__init__(
            endpoint=endpoint,
            token=token,
            library_name=library_name,
            library_version=library_version,
            user_agent=user_agent,
            headers=headers,
        )
        self._explicit_token = token

    def _resolve_token(self, override):
        if override is not None:
            return override
        return self._explicit_token

    def _inject(self, kwargs):
        kwargs.setdefault("token", self._resolve_token(kwargs.get("token")))
        if self.endpoint is not None:
            kwargs.setdefault("endpoint", self.endpoint)
        return kwargs

    # ---- Phase A ----

    def hf_hub_download(self, repo_id, filename, **kwargs):
        """Download a single file from the Hippius OCI registry (delegates to `file_download.hf_hub_download`)."""
        return _hf_hub_download(repo_id, filename, **self._inject(kwargs))

    def snapshot_download(self, repo_id, **kwargs):
        """Download every file in `repo_id` at a revision (delegates to `_snapshot_download.snapshot_download`)."""
        return _snapshot_download(repo_id, **self._inject(kwargs))

    def whoami(self, token=None):
        """Return the authenticated user/robot identity from Harbor."""
        return _whoami_module(
            token=token if token is not None else self._explicit_token,
            endpoint=self.endpoint,
        )

    # ---- Phase B uploads ----

    def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, **kwargs):
        """Upload a single file into `repo_id` (delegates to `file_upload.upload_file`)."""
        return _upload_file(
            path_or_fileobj=path_or_fileobj,
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            **self._inject(kwargs),
        )

    def upload_folder(self, *, repo_id, folder_path, **kwargs):
        """Upload every file under `folder_path` into `repo_id` (delegates to `file_upload.upload_folder`)."""
        return _upload_folder(
            repo_id=repo_id,
            folder_path=folder_path,
            **self._inject(kwargs),
        )

    # ---- Phase B repo CRUD + inspection ----

    def create_repo(self, repo_id, **kwargs):
        """Ensure the Harbor project for `repo_id` exists (delegates to `_repo_ops.create_repo`)."""
        return _create_repo(repo_id, **self._inject(kwargs))

    def delete_repo(self, repo_id, **kwargs):
        """Delete the Harbor repository for `repo_id` (delegates to `_repo_ops.delete_repo`)."""
        return _delete_repo(repo_id, **self._inject(kwargs))

    def repo_info(self, repo_id, **kwargs):
        """Return a ModelInfo describing `repo_id` (delegates to `_repo_ops.repo_info`)."""
        return _repo_info(repo_id, **self._inject(kwargs))

    def model_info(self, repo_id, **kwargs):
        """Return a ModelInfo for a model repo (delegates to `_repo_ops.model_info`)."""
        return _model_info(repo_id, **self._inject(kwargs))

    def list_repo_files(self, repo_id, **kwargs):
        """Return the file list for `repo_id` at a revision (delegates to `_repo_ops.list_repo_files`)."""
        return _list_repo_files(repo_id, **self._inject(kwargs))

    def repo_exists(self, repo_id, **kwargs):
        """True iff `repo_id` has at least one pushed tag (delegates to `_repo_ops.repo_exists`)."""
        return _repo_exists(repo_id, **self._inject(kwargs))

    def revision_exists(self, repo_id, revision, **kwargs):
        """True iff `repo_id:revision` has a manifest (delegates to `_repo_ops.revision_exists`)."""
        return _revision_exists(repo_id, revision, **self._inject(kwargs))

    def file_exists(self, repo_id, filename, **kwargs):
        """True iff `filename` is in `repo_id`'s manifest at a revision (delegates to `_repo_ops.file_exists`)."""
        return _file_exists(repo_id, filename, **self._inject(kwargs))

    # ---- Auth pass-throughs ----

    def login(self, *args, **kwargs):
        """Persist credentials for subsequent registry calls (delegates to `auth.login`)."""
        return _login_module(*args, **kwargs)

    def logout(self, *args, **kwargs):
        """Forget any persisted credentials (delegates to `auth.logout`)."""
        return _logout_module(*args, **kwargs)


# Names that should NOT be auto-stubbed: methods we override above, plus
# methods we deliberately inherit from HfApi (composer/dispatcher helpers).
_OVERRIDDEN = set(HippiusApi.__dict__.keys())
_INHERITED_OK = {
    "run_as_future",  # dispatches to other methods; works as long as the target method works
}


def _stub_method(method_name: str):
    def stub(self, *args, **kwargs):
        raise NotImplementedError(
            f"HippiusApi.{method_name}() is HF-specific and not supported by hippius_hub. "
            f"This API exists on huggingface_hub.HfApi but the Hippius OCI-backed registry "
            f"does not provide an equivalent feature."
        )
    stub.__name__ = method_name
    stub.__qualname__ = f"HippiusApi.{method_name}"
    # Mirror the HfApi method's identity in the docstring so callers see at a
    # glance that this is an intentional no-op rather than a missing attribute.
    stub.__doc__ = (
        f"Not supported by hippius_hub: HfApi.{method_name}() has no equivalent "
        f"on the OCI-backed Hippius registry; calling raises NotImplementedError."
    )
    return stub


def _install_stubs():
    """Attach a NotImplementedError stub for every public HfApi method we
    don't implement and don't inherit. Wrapped in a function so the loop
    variables don't leak into the module namespace."""
    for name in dir(HfApi):
        if name.startswith("_"):
            continue
        if name in _OVERRIDDEN or name in _INHERITED_OK:
            continue
        attr = getattr(HfApi, name, None)
        if not callable(attr) or isinstance(attr, property):
            continue
        setattr(HippiusApi, name, _stub_method(name))


_install_stubs()
del _install_stubs


__all__ = ["HippiusApi", "ModelCard"]
