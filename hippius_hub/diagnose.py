"""Transfer-speed diagnostics for a single file.

Runs a phased probe — auth token service, metadata lookup, then the file
transfer itself (single-stream vs parallel throughput) — and renders a
shareable report ending in a plain-English verdict. When a user reports slow
push/pull, have them run this and paste the output: the per-phase timings tell
us whether the time is going to setup or to the bytes, and the server
request-ids let us line the report up against our own logs.

The output is deliberately generic about the backend (it talks about a
"registry", a "metadata lookup", an "auth token service", a "download host")
so it stays safe to show end users.
"""
import json
import platform
import os
import time
from typing import Callable, Optional, Tuple

from . import __version__
from ._logging import configure_logging
from ._oci import fetch_manifest, layer_title
from .auth import get_oci_bearer_token, resolve_token_value
from .constants import (
    resolve_chunk_size,
    resolve_connect_timeout,
    resolve_max_concurrent,
    resolve_read_timeout,
    resolve_registry,
)
from .errors import EntryNotFoundError
from .file_download import _oci_repo_path, _validate_repo_type

try:
    from .hippius_core import diagnose_blob_native
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


_DEFAULT_PROBE_BYTES = 32 * 1024 * 1024  # 32 MB

_PROXY_ENV_VARS = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)


def _detect_proxies() -> dict:
    return {k: os.environ[k] for k in _PROXY_ENV_VARS if os.environ.get(k)}


def _env_summary() -> dict:
    return {
        "hippius_hub_version": __version__,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "chunk_size": resolve_chunk_size(),
        "max_concurrent": resolve_max_concurrent(),
        "connect_timeout": resolve_connect_timeout(),
        "read_timeout": resolve_read_timeout(),
        "proxies": _detect_proxies(),
    }


def _timed(fn: Callable, *args, **kwargs) -> Tuple[object, float]:
    """Call `fn`, returning (result, elapsed_ms). Errors bubble — a failed or
    hung phase is itself the diagnosis."""
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, (time.perf_counter() - t0) * 1000.0


def run_diagnose(
    repo_id: str,
    filename: str,
    *,
    revision: str = "main",
    repo_type: Optional[str] = None,
    endpoint: Optional[str] = None,
    token=None,
    probe_bytes: int = _DEFAULT_PROBE_BYTES,
) -> dict:
    """Probe the path a download takes for `filename` and return a report dict."""
    logger = configure_logging()
    _validate_repo_type(repo_type)

    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    auth_token = resolve_token_value(token)

    report = {
        "repo_id": repo_id,
        "filename": filename,
        "revision": revision,
        "registry": registry,
        "env": _env_summary(),
    }

    logger.debug("requesting auth token for %s", oci_repo)
    oci_token, token_ms = _timed(get_oci_bearer_token, oci_repo, auth_token)
    report["token"] = {"ms": round(token_ms, 1)}

    logger.debug("fetching metadata for %s:%s", oci_repo, revision)
    manifest_result, manifest_ms = _timed(fetch_manifest, registry, oci_repo, revision, oci_token)
    layers = manifest_result.manifest.get("layers", [])
    report["manifest"] = {"ms": round(manifest_ms, 1), "files": len(layers)}

    target_digest = None
    for layer in layers:
        if layer_title(layer) == filename:
            target_digest = layer.get("digest")
            break
    if not target_digest:
        raise EntryNotFoundError(
            f"File '{filename}' not found in '{repo_id}:{revision}'"
        )

    blob_url = f"{registry}/v2/{oci_repo}/blobs/{target_digest}"

    logger.debug("probing transfer from %s", blob_url)
    raw = diagnose_blob_native(
        blob_url=blob_url,
        auth_token=oci_token,
        probe_bytes=probe_bytes,
        max_concurrent=resolve_max_concurrent(),
        connect_timeout_secs=resolve_connect_timeout(),
        # Wire the read/idle timeout into the probe (audit M-DIAG-TIMEOUT): it now
        # bounds a stalled read so `diagnose` cannot hang forever, and makes
        # HIPPIUS_READ_TIMEOUT a real knob for the probe instead of dead config.
        read_timeout_secs=int(resolve_read_timeout()),
    )
    report["blob"] = json.loads(raw)
    report["verdict"] = _verdict(report)
    return report


def _verdict(report: dict) -> list:
    """Plain-English conclusions from the collected signals."""
    blob = report["blob"]
    lines = []

    single = blob.get("single_stream_mbps")
    parallel = blob.get("parallel_mbps")
    rtt = blob.get("rtt_ms")
    chunks = blob.get("parallel_chunks") or []

    if single and parallel:
        if parallel >= single * 3:
            lines.append(
                "Single-connection throughput is much lower than parallel — this is "
                "a high-latency link, and using many parallel connections is "
                "recovering your throughput as intended. Expected for distant "
                "connections; nothing is wrong on the server side."
            )
        elif rtt is not None and rtt < 30 and parallel < 20:
            lines.append(
                "Throughput is low even with parallelism, despite low latency to "
                "the server. The bottleneck is not network distance — check whether "
                "a VPN/proxy or your local link is the limit."
            )

    if len(chunks) >= 4:
        half = len(chunks) // 2
        early = chunks[:half]
        late = chunks[half:]
        early_avg = sum(c["mbps"] for c in early) / len(early) if early else 0.0
        late_avg = sum(c["mbps"] for c in late) / len(late) if late else 0.0
        if early_avg > 0 and late_avg < early_avg * 0.5:
            lines.append(
                "Throughput collapses partway through the transfer (later parts much "
                "slower than the start) — consistent with rate-limiting on your "
                "connection or a saturated download host."
            )

    if blob.get("redirected") and blob.get("final_host"):
        lines.append(
            f"Downloads are served from {blob['final_host']}; the region of that "
            f"host largely determines your speed."
        )

    proxies = report["env"].get("proxies") or {}
    if proxies:
        names = ", ".join(sorted(proxies))
        lines.append(
            f"A network proxy is configured ({names}). Proxies frequently cap or "
            f"serialize transfers — try again without it to compare."
        )

    if not lines:
        lines.append(
            "No obvious bottleneck detected — throughput is consistent with the "
            "measured latency to the server."
        )
    return lines


# ----- report formatting -----

def _fmt_bytes(n) -> str:
    if n is None:
        return "—"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _ms(v) -> str:
    return "—" if v is None else f"{v:.0f} ms"


def _mbps(v) -> str:
    return "—" if v is None else f"{v:.1f} MB/s"


def format_report(report: dict) -> str:
    env = report["env"]
    blob = report["blob"]
    out = []

    out.append(
        f"Transfer diagnostics for {report['repo_id']}/{report['filename']} "
        f"(revision: {report['revision']})"
    )

    out.append("")
    out.append("== Environment ==")
    out.append(f"  hippius-hub:   {env['hippius_hub_version']}")
    out.append(f"  platform:      {env['platform']}")
    out.append(f"  python:        {env['python']}")
    out.append(f"  chunk size:    {_fmt_bytes(env['chunk_size'])}")
    out.append(f"  concurrency:   {env['max_concurrent']}")
    out.append(f"  connect timeout: {env['connect_timeout']}s")
    if env.get("read_timeout") is not None:
        out.append(f"  read timeout:  {env['read_timeout']}s")
    if env.get("proxies"):
        out.append(f"  proxies:       {', '.join(sorted(env['proxies']))}")

    out.append("")
    out.append("== Endpoint ==")
    out.append(f"  registry:      {report['registry']}")
    out.append(f"  resolved IP:   {blob.get('resolved_ip') or '—'}")
    out.append(f"  DNS lookup:    {_ms(blob.get('dns_ms'))}")
    out.append(f"  TCP connect:   {_ms(blob.get('tcp_connect_ms'))}  (latency ~{_ms(blob.get('rtt_ms'))})")
    tls = " ".join(x for x in (blob.get("tls_version"), blob.get("alpn")) if x)
    out.append(f"  TLS handshake: {_ms(blob.get('tls_handshake_ms'))}  {tls}".rstrip())
    out.append(f"  HTTP version:  {blob.get('http_version') or '—'}")

    out.append("")
    out.append("== Control plane ==")
    out.append(f"  auth token service: {_ms(report['token']['ms'])}")
    out.append(f"  metadata lookup:    {_ms(report['manifest']['ms'])} ({report['manifest']['files']} files)")

    out.append("")
    out.append("== File transfer ==")
    if blob.get("redirected"):
        out.append(f"  redirect to download host: {blob.get('final_host') or blob.get('location') or '—'}")
    out.append(f"  probe size:    {_fmt_bytes(blob.get('probe_bytes'))}")
    out.append(f"  time to first byte: {_ms(blob.get('ttfb_ms'))}")
    out.append(f"  single connection:  {_mbps(blob.get('single_stream_mbps'))}")
    out.append(f"  parallel ({blob.get('max_concurrent')} conns): {_mbps(blob.get('parallel_mbps'))}")
    chunks = blob.get("parallel_chunks") or []
    if chunks:
        slowest = max(chunks, key=lambda c: c["ms"])
        out.append(
            f"  slowest part:  #{slowest['index']} {_ms(slowest['ms'])} ({_mbps(slowest['mbps'])})"
        )
    rids = blob.get("server_request_ids") or {}
    if rids:
        out.append("  server request ids (share with support):")
        for k, v in rids.items():
            out.append(f"    {k}: {v}")
    for note in blob.get("errors") or []:
        out.append(f"  note: {note}")

    out.append("")
    out.append("== Verdict ==")
    for line in report.get("verdict", []):
        out.append(f"  • {line}")

    return "\n".join(out)
