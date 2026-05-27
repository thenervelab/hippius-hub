"""Regression: hf_hub_download and snapshot_download must surface their
'accepted but ignored' kwargs via UserWarning (or raise for dry_run on
hf_hub_download — it's not supported there)."""
import pytest
from hippius_hub.file_download import hf_hub_download
from hippius_hub._snapshot_download import snapshot_download


def test_hf_hub_download_dry_run_raises():
    """dry_run=True on hf_hub_download must NOT silently download."""
    with pytest.raises(NotImplementedError, match="dry_run"):
        hf_hub_download("foo/bar", "x", dry_run=True)


def test_hf_hub_download_warns_on_tqdm_class():
    """tqdm_class is accept-and-ignore; user should see a warning."""
    # tmp_path not needed — the warning fires BEFORE any network call.
    with pytest.warns(UserWarning, match="tqdm_class"):
        try:
            hf_hub_download("foo/bar", "x", tqdm_class=object)
        except Exception:
            # The eventual network/auth failure is irrelevant — we only
            # care that the warning fired during arg processing.
            pass


def test_hf_hub_download_warns_on_headers():
    with pytest.warns(UserWarning, match="headers"):
        try:
            hf_hub_download("foo/bar", "x", headers={"X-Custom": "v"})
        except Exception:
            pass


def test_hf_hub_download_warns_on_etag_timeout_non_default():
    with pytest.warns(UserWarning, match="etag_timeout"):
        try:
            hf_hub_download("foo/bar", "x", etag_timeout=30.0)
        except Exception:
            pass


def test_hf_hub_download_default_etag_timeout_does_not_warn():
    """Default value 10.0 must not trigger a warning."""
    import warnings as w
    with w.catch_warnings(record=True) as caught:
        w.simplefilter("always")
        try:
            hf_hub_download("foo/bar", "x")  # default etag_timeout=10.0
        except Exception:
            pass
        etag_warnings = [str(x.message) for x in caught if "etag_timeout" in str(x.message)]
        assert etag_warnings == []


def test_snapshot_download_dry_run_does_not_raise():
    """snapshot_download supports dry_run — must NOT raise NotImplementedError.

    We swallow other exceptions (network/auth) — they're irrelevant to the
    contract under test, which is purely 'dry_run is not in the rejected-kwargs
    set'. The dry_run-NotImplementedError check is the asserting branch.
    """
    try:
        snapshot_download("foo/bar", dry_run=True)
    except NotImplementedError as e:
        if "dry_run" in str(e):
            pytest.fail("snapshot_download must support dry_run")
        # Some other NotImplementedError (e.g. unsupported repo_type) — not
        # our concern here.
    except Exception:
        # Network / auth / manifest-not-found — the warning + kwarg handling
        # already happened before any network call. The contract under test
        # is satisfied.
        pass


def test_snapshot_download_warns_on_tqdm_class():
    with pytest.warns(UserWarning, match="tqdm_class"):
        try:
            snapshot_download("foo/bar", tqdm_class=object)
        except Exception:
            pass
