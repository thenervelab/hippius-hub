"""The hub refuses this client's model pushes, and says what to use instead.

`registry.hippius.com`'s projects moved from Harbor to the Hippius Hub (`tests/_hub.py`).
The hub stores container images through the registry API and models through the Hugging
Face API, so it refuses a model pushed the way this client pushes one: at the first request
to a name that holds a model, with `DENIED` and a message naming `huggingface_hub`
(hippius-hub-rust `crates/hub-api/src/oci/push.rs`, `model_push_refused`). The tests marked
`MOVES_TO_W12` fail for this reason; these tests pin the reason itself, so a different
failure there is not mistaken for it.
"""

import base64
import uuid

import httpx
import pytest

from hippius_hub import list_repo_refs, upload_file
from hippius_hub.constants import resolve_registry

# The part of the hub's message that names the cause; the rest names the fix and a URL.
REFUSAL = "the registry API takes container images, not models"


def _credential_header(creds) -> str:
    if creds["user"] and creds["password"]:
        pair = f"{creds['user']}:{creds['password']}".encode()
        return "Basic " + base64.b64encode(pair).decode()
    return f"Bearer {creds['token']}"


@pytest.mark.e2e
def test_registry_refuses_upload_to_model_repo(creds, test_repo):
    """A push token is issued (the key may write), and the first push request is refused
    with the hub's message, not a 401 or a 5xx."""
    registry = resolve_registry(None)
    scope = f"repository:{test_repo}:pull,push"
    token_resp = httpx.get(
        f"{registry}/service/token",
        params={"service": "harbor-registry", "scope": scope},
        headers={"Authorization": _credential_header(creds)},
        timeout=30,
    )
    assert token_resp.status_code == 200, (
        f"token for {scope} answered {token_resp.status_code}: the key was not accepted"
    )
    token = token_resp.json()["token"]

    resp = httpx.post(
        f"{registry}/v2/{test_repo}/blobs/uploads/",
        headers={"Authorization": f"Bearer {token}", "Content-Length": "0"},
        timeout=30,
    )
    assert resp.status_code == 403, f"expected 403, got {resp.status_code}: {resp.text[:300]}"
    error = resp.json()["errors"][0]
    assert error["code"] == "DENIED"
    assert REFUSAL in error["message"]
    assert "huggingface_hub" in error["message"]


@pytest.mark.e2e
def test_client_upload_is_refused_and_leaves_no_revision(tmp_path, logged_in, test_repo):
    """The client's own push raises, and the revision it named does not appear. The
    client does not pass the hub's message through, which the raw test above checks."""
    src = tmp_path / "refused.bin"
    src.write_bytes(b"refused")
    revision = f"refused-{uuid.uuid4().hex[:8]}"

    with pytest.raises(Exception, match="403"):
        upload_file(
            path_or_fileobj=str(src),
            path_in_repo="refused.bin",
            repo_id=test_repo,
            revision=revision,
        )

    refs = list_repo_refs(test_repo)
    names = {r.name for r in refs.tags} | {b.name for b in refs.branches}
    assert revision not in names
