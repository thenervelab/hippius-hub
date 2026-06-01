# Diagnosing slow downloads / uploads

Some users get great throughput, others are slow — usually depending on where
they connect from (location, VPS, VPN, ISP). This is a triage guide for turning
"it's slow" into a root cause.

## First: get a report

Have the user run the built-in probe and paste the output:

```bash
hippius-hub diagnose <repo_id> <filename>
# more detail (per-chunk transport logs on stderr):
hippius-hub diagnose <repo_id> <filename> --verbose
# machine-readable:
hippius-hub diagnose <repo_id> <filename> --json
```

The report breaks the transfer into phases (endpoint handshake → auth token
service → metadata lookup → file transfer) and ends in a plain-English verdict.

### Reading it

- **single connection vs parallel** — the headline. If single-connection
  throughput is low but the parallel number is several times higher, the user
  is bandwidth-delay-product limited on a high-latency link, and our many
  parallel connections are *already* the fix. This is expected for distant
  connections and is not a server problem.
- **latency (~RTT)** — high RTT with low single-stream throughput is the
  classic high-BDP signature; parallelism is the mitigation.
- **slowest part / per-chunk timing (`--verbose`)** — if early chunks are fast
  and later ones collapse, suspect ISP token-bucket policing or a saturated
  download host.
- **redirect to download host** — if present, that host's region determines
  the user's speed; note it.
- **proxies** — a configured `HTTP(S)_PROXY` frequently caps or serializes
  transfers; have them retry without it.
- **server request ids** (`x-amz-request-id`, `x-amz-id-2`, …) — copy these to
  cross-reference our server logs (see below).

### Tuning knobs to A/B

All overridable via env (also `--chunk-size` on `download`):

| Env var | Default | Effect |
|---|---|---|
| `HIPPIUS_MAX_CONCURRENT` | 32 | parallel connections per file |
| `HIPPIUS_CHUNK_SIZE` | 100 MB | bytes per ranged request |
| `HIPPIUS_CONNECT_TIMEOUT` | 30 | TCP connect timeout (s) |
| `HIPPIUS_READ_TIMEOUT` | unset | per-chunk total timeout (s); opt-in |
| `HIPPIUS_SNAPSHOT_WORKERS` | 8 | concurrent files in `snapshot_download` |
| `HIPPIUS_UPLOAD_WORKERS` | 8 | concurrent files in folder upload |
| `HIPPIUS_DEBUG=1` / `RUST_LOG` | off | verbose transport logging |

Quick experiment to confirm a BDP-limited link: `HIPPIUS_MAX_CONCURRENT=1`
versus the default 32 — the single-vs-parallel gap should be obvious.

## Client-side deep dive (when the report isn't enough)

Top-down — find which layer is slow before blaming the network:

1. **`curl -w` phase breakdown** — is the time in DNS, TCP connect, TLS, or the
   body transfer?
   ```bash
   curl -o /dev/null -s -w 'dns:%{time_namelookup} conn:%{time_connect} tls:%{time_appconnect} ttfb:%{time_starttransfer} total:%{time_total} speed:%{speed_download}\n' <blob_url>
   ```
   If `time_total - time_starttransfer` dominates, it's the body transfer
   (bandwidth/path); otherwise it's setup/server.
2. **`mtr` / `tcptraceroute`** — localize latency/loss to a hop. Run both
   directions if possible (paths are asymmetric). A single hop with loss that
   *persists* on later hops matters; transient mid-path loss is usually a red
   herring (routers rate-limiting ICMP).
   ```bash
   mtr -n -T -P 443 --report --report-cycles 100 registry.hippius.com
   ```
3. **`iperf3`** (between hosts you control) — separates "the network is slow"
   from "this one flow is shaped". Test single vs parallel streams and the
   download direction:
   ```bash
   iperf3 -c <server> -t 15 -R        # download direction
   iperf3 -c <server> -t 15 -R -P 8   # parallel
   ```
   Poor single-stream but good parallel = flow-sensitive path (window/BDP), not
   raw capacity — exactly what our parallel downloader is built for.

## Server-side deep dive (we operate the registry + backend)

1. **Raise the registry log level to debug** and reproduce the slow transfer to
   see the actual backend (object-storage) operations, not just the front-end
   requests.
2. **Grep for the report's request-ids** in the registry / object-storage logs
   to find the exact request and its backend timing.
3. **Look for backend throttling** — `SlowDown` / HTTP 429 / 503 from the
   object store during large or parallel transfers. If present, raise the
   storage-driver chunk size and/or the rate limits.
4. **Confirm region / POP** — pull the same file from a VM in the registry's
   region vs the user's region to isolate network distance from server limits.
5. **Check the download host** — if blobs are served via a redirect to object
   storage / CDN, confirm the user is hitting a nearby edge, not a distant
   origin.

## Common root causes (location-dependent speed)

- **High RTT + undersized TCP window** (high BDP): single-stream caps out; our
  32-way parallelism is the intended mitigation. Verdict will say so.
- **ISP token-bucket policing**: fast start, then collapse mid-transfer.
- **VPN exit congestion / CPU-bound encryption**: varies by exit node and time
  of day; have them try a different exit or no VPN.
- **Proxy in the path**: caps/serializes; flagged in the report.
- **Single-region origin, no nearby edge**: distant users are RTT-bound.
