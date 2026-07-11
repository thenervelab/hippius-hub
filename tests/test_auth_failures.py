"""Negative-path coverage: invalid creds, missing repo, role-scoped denial.

Today, all e2e tests use a fully-authorized fixture. A regression in the
auth flow (e.g. silently swallowing a 401, sending no Authorization header,
not enforcing role) would slip through. These tests pin the *typed* error
shape callers expect — `ConsoleError(401)` for bad console tokens,
`ConsoleError(404)` for missing resources, an explicit auth error when a
read-only key tries to push.
"""
import os
import uuid

import httpx
import pytest

from hippius_hub import console, upload_file
from hippius_hub.console import ConsoleError

from tests._helpers import write_test_file


pytestmark = pytest.mark.e2e


# ---------- console API auth failures ----------

def test_console_call_with_garbage_token_raises_401(tmp_path, monkeypatch):
    """A junk token must produce ConsoleError(401) — not a 5xx, not a hang,
    not a silent empty result."""
    monkeypatch.setattr(console, "API_TOKEN_PATH", str(tmp_path / "api_token"))
    console.save_api_token("garbage-not-a-real-token")

    with pytest.raises(ConsoleError) as excinfo:
        console.me()
    assert excinfo.value.status_code == 401


def test_console_call_without_token_raises_401(tmp_path, monkeypatch):
    """No saved token AND no token argument: the `_headers(require=True)`
    branch must short-circuit with the "Not logged in" 401 before any
    network call."""
    monkeypatch.setattr(console, "API_TOKEN_PATH", str(tmp_path / "no-such-token"))
    with pytest.raises(ConsoleError) as excinfo:
        console.me()
    assert excinfo.value.status_code == 401
    assert "logged in" in str(excinfo.value.body).lower()


def test_console_get_artifact_unknown_repo_raises_404(console_logged_in, console_test_project):
    """A repo that doesn't exist under the active project must surface a
    typed 404 — CLI users see `❌ Not found: …`."""
    ghost = f"ghost-{uuid.uuid4().hex[:8]}"
    with pytest.raises(ConsoleError) as excinfo:
        console.get_artifact(f"{console_test_project}/{ghost}", "main")
    assert excinfo.value.status_code == 404


def test_console_check_namespace_works_without_auth(tmp_path, monkeypatch):
    """`registry plans` and `check_namespace` are documented as
    `require_auth=False`. Verify the unauthenticated branch actually works
    so anonymous discovery from the CLI keeps functioning."""
    monkeypatch.setattr(console, "API_TOKEN_PATH", str(tmp_path / "no-token"))
    # No token saved; this must succeed.
    plans = console.list_plans()
    assert isinstance(plans, list) and plans


# ---------- OCI auth failures ----------

def test_oci_call_with_garbage_token_fails(tmp_path, monkeypatch, test_repo):
    """A bogus docker-registry token must produce an httpx error when used
    for a PUSH operation.

    Why push and not pull: `test/e2e-client` is a public repo (so anonymous
    pull tokens are issued even with garbage credentials — the registry just
    drops the bogus Authorization header and treats the request as anonymous).
    The push scope, however, requires authenticated identity, so the token
    endpoint refuses it for an invalid Basic header. This is the genuine
    "auth is actually evaluated" signal."""
    from hippius_hub import auth, upload_file

    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "tok"))
    with open(auth.TOKEN_PATH, "w") as f:
        # Looks like a Basic header but the credentials are nonsense.
        f.write("Basic " + "Z2FyYmFnZTpnYXJiYWdl")  # garbage:garbage

    src = tmp_path / "garbage-auth.bin"
    src.write_bytes(b"x")

    # The bogus token is refused at one of two layers, both of which count as
    # "auth is actually evaluated":
    #   - the token endpoint refuses push scope -> httpx.HTTPStatusError(401)
    #     raised by get_oci_bearer_token; or
    #   - it issues a no-perm token and the registry rejects the blob-upload
    #     session-init (or PUT), which the native uploader surfaces as a
    #     RuntimeError carrying the status (401/403). The session-init POST
    #     lives in Rust now — see `_ensure_blob_uploaded` — so this arm is a
    #     RuntimeError, not an httpx error.
    with pytest.raises((httpx.HTTPStatusError, RuntimeError)) as excinfo:
        upload_file(
            path_or_fileobj=str(src),
            path_in_repo="garbage-auth.bin",
            repo_id=test_repo,
            revision=f"garbage-{uuid.uuid4().hex[:8]}",
        )
    exc = excinfo.value
    if isinstance(exc, httpx.HTTPStatusError):
        assert exc.response.status_code in (401, 403)
    else:
        assert "401" in str(exc) or "403" in str(exc), (
            f"native uploader must surface the auth status, got: {exc}"
        )


# ---------- role enforcement ----------

@pytest.fixture
def readonly_robot_creds(console_logged_in):
    """Create a `read` role key for this test, hand back (login, secret),
    revoke in teardown so the test account doesn't accumulate robots.

    If key creation fails (account lacks the perm or quota), skip rather
    than leak — role enforcement is the test's whole point but a non-issuable
    key isn't a code bug here."""
    name = f"e2e-readonly-{uuid.uuid4().hex[:8]}"
    try:
        created = console.create_key(name, "read")
    except ConsoleError as e:
        pytest.skip(f"Couldn't create read-only key: {e}")
    try:
        yield created["login"], created["secret"]
    finally:
        try:
            console.revoke_key(created["id"])
        except Exception:
            pass


def test_readonly_key_cannot_push(
    tmp_path, monkeypatch, readonly_robot_creds, test_repo,
):
    """A `role=read` key must be rejected at push time. Validates that the
    role preset actually maps to a Harbor ACL that denies write — without
    this, the role labels are decorative."""
    import base64
    from hippius_hub import auth

    login, secret = readonly_robot_creds
    basic = base64.b64encode(f"{login}:{secret}".encode()).decode()

    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "ro_tok"))
    with open(auth.TOKEN_PATH, "w") as f:
        f.write(f"Basic {basic}")

    src = tmp_path / "ro.bin"
    write_test_file(src, 64, seed=b"ro")

    with pytest.raises(Exception) as excinfo:
        upload_file(
            path_or_fileobj=str(src),
            path_in_repo="ro.bin",
            repo_id=test_repo,
            revision=f"ro-{uuid.uuid4().hex[:8]}",
        )
    # 401 (token-service refused push scope) or 403 (push blocked by ACL).
    # Both are valid "you don't have push" responses — the contract is "this
    # call must raise", not the exact HTTP status.
    err = str(excinfo.value).lower()
    assert any(s in err for s in ("401", "403", "denied", "forbidden", "unauthorized")), (
        f"expected an auth error, got: {excinfo.value!r}"
    )
