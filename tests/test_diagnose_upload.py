"""Pure rendering/verdict logic for the upload throughput probe.

The probe itself hits the network (covered by the Rust wiremock tests); here we
pin the interpretation layer — the ratio-based verdict and the report format —
which is what an operator reads to decide the deployment gate.
"""
from hippius_hub.diagnose import _upload_verdict, format_upload_report


def _report(single, parallel, errors=None):
    report = {
        "url": "http://receiver.test",
        "upload": {
            "probe_bytes": 32 * 1024 * 1024,
            "single_stream_mbps": single,
            "single_stream_ms": 100,
            "parallel_mbps": parallel,
            "parallel_ms": 40,
            "max_concurrent": 16,
            "errors": errors or [],
        },
    }
    report["verdict"] = _upload_verdict(report)
    return report


def test_verdict_flags_parallel_win():
    verdict = _report(10.0, 40.0)["verdict"]
    assert any("meaningfully helps" in line for line in verdict), verdict


def test_verdict_flags_marginal_gain():
    verdict = _report(30.0, 33.0)["verdict"]
    assert any("buys little" in line for line in verdict), verdict


def test_verdict_surfaces_probe_errors():
    verdict = _report(None, None, errors=["probe_bytes is 0; nothing to upload"])["verdict"]
    assert any("nothing to upload" in line for line in verdict), verdict


def test_format_renders_both_throughputs():
    out = format_upload_report(_report(10.0, 40.0))
    assert "single stream:" in out
    assert "parallel (16 conns):" in out
    assert "MB/s" in out
    assert "Verdict" in out
