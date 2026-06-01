import os
import subprocess
import sys

import pytest

from hippius_hub.diagnose import _verdict, format_report


HIPPIUS_CLI = [sys.executable, "-m", "hippius_hub.cli"]


def _sample_report(**blob_overrides):
    """A synthetic diagnose report, shaped like run_diagnose() output, for
    testing formatting/verdict logic without touching the network."""
    blob = {
        "scheme": "https",
        "host": "registry.hippius.com",
        "port": 443,
        "resolved_ip": "203.0.113.7",
        "dns_ms": 12,
        "tcp_connect_ms": 85,
        "rtt_ms": 85,
        "tls_handshake_ms": 91,
        "tls_version": "TLSv1_3",
        "alpn": "http/1.1",
        "head_status": 200,
        "redirected": False,
        "location": None,
        "final_host": None,
        "http_version": "HTTP/1.1",
        "content_length": 536870912,
        "server_request_ids": {"x-amz-request-id": "ABC123", "x-amz-id-2": "ZZZ"},
        "ttfb_ms": 110,
        "probe_bytes": 33554432,
        "single_stream_mbps": 15.6,
        "parallel_mbps": 82.0,
        "parallel_ms": 6200,
        "single_stream_ms": 4100,
        "max_concurrent": 32,
        "parallel_chunks": [
            {"index": i, "bytes": 1048576, "ms": 200, "mbps": 5.0} for i in range(8)
        ],
        "errors": [],
    }
    blob.update(blob_overrides)
    return {
        "repo_id": "org/model",
        "filename": "weights.safetensors",
        "revision": "main",
        "registry": "https://registry.hippius.com",
        "env": {
            "hippius_hub_version": "0.4.7",
            "platform": "macOS-15",
            "python": "3.13.7",
            "chunk_size": 104857600,
            "max_concurrent": 32,
            "connect_timeout": 30,
            "read_timeout": None,
            "proxies": {},
        },
        "token": {"ms": 210.0},
        "manifest": {"ms": 180.0, "files": 3},
        "blob": blob,
    }


def test_format_report_is_backend_generic():
    """The report must not leak backend-implementation terms — it's meant to be
    pasted by end users."""
    report = _sample_report()
    report["verdict"] = _verdict(report)
    text = format_report(report).lower()

    assert "harbor" not in text
    assert "oci" not in text
    assert "manifest" not in text  # we say "metadata lookup" instead


def test_format_report_has_key_signals():
    report = _sample_report()
    report["verdict"] = _verdict(report)
    text = format_report(report)

    # Headline throughput numbers and the shareable request-ids are present.
    assert "single connection" in text
    assert "parallel" in text
    assert "MB/s" in text
    assert "203.0.113.7" in text
    assert "x-amz-request-id" in text
    assert "== Verdict ==" in text


def test_verdict_high_latency_parallel_recovery():
    # single slow, parallel >= 3x → expected "parallelism recovering" verdict.
    report = _sample_report(single_stream_mbps=15.6, parallel_mbps=82.0)
    lines = _verdict(report)
    joined = " ".join(lines).lower()
    assert "parallel" in joined and "recover" in joined


def test_verdict_low_throughput_low_latency():
    report = _sample_report(single_stream_mbps=4.0, parallel_mbps=6.0, rtt_ms=10)
    lines = _verdict(report)
    joined = " ".join(lines).lower()
    assert "bottleneck is not network distance" in joined


def test_verdict_flags_redirect_host():
    report = _sample_report(redirected=True, final_host="cdn.example.com")
    lines = _verdict(report)
    assert any("cdn.example.com" in line for line in lines)


def test_verdict_flags_collapse():
    # Early chunks fast, late chunks collapse → rate-policing signal.
    chunks = [{"index": i, "bytes": 1048576, "ms": 100, "mbps": 10.0} for i in range(4)]
    chunks += [{"index": i + 4, "bytes": 1048576, "ms": 1000, "mbps": 1.0} for i in range(4)]
    report = _sample_report(parallel_chunks=chunks)
    lines = _verdict(report)
    joined = " ".join(lines).lower()
    assert "collapses" in joined


def test_cli_diagnose_help_parses():
    result = subprocess.run(
        HIPPIUS_CLI + ["diagnose", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "throughput" in result.stdout


@pytest.mark.e2e
def test_cli_diagnose_smoke(tmp_path, test_repo, creds, revision):
    src = tmp_path / "diag.bin"
    src.write_bytes(b"hippius-diagnose-smoke\n" * 4096)

    env = {**os.environ, "HOME": str(tmp_path)}
    (tmp_path / ".cache" / "hippius" / "hub").mkdir(parents=True)

    if creds.get("user") and creds.get("password"):
        login_cmd = HIPPIUS_CLI + ["login", "--username", creds["user"], "--password", creds["password"]]
    else:
        login_cmd = HIPPIUS_CLI + ["login", "--token", creds["token"]]
    subprocess.run(login_cmd, env=env, check=True, capture_output=True)

    subprocess.run(
        HIPPIUS_CLI + ["upload", test_repo, str(src), "--revision", revision],
        env=env, check=True, capture_output=True,
    )

    result = subprocess.run(
        HIPPIUS_CLI + ["diagnose", test_repo, "diag.bin", "--revision", revision, "--probe-mb", "1"],
        env=env, check=True, capture_output=True, text=True,
    )
    assert "MB/s" in result.stdout
    assert "== Verdict ==" in result.stdout
