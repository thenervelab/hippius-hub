# hub-blob-receiver — deployment

The in-cluster staged blob receiver. It terminates the hippius-hub client's N
concurrent WAN part streams, stages them on local NVMe, and on completion
streams one native OCI blob PUT into Harbor on the LAN ("parallelize the WAN,
serialize the LAN"). Design: `docs/plans/2026-07-09-parallel-blob-upload.md`.

## Build & publish

```bash
# From the repo root (build context is the whole workspace):
docker build -f Dockerfile.receiver -t ghcr.io/thenervelab/hub-blob-receiver:<tag> .
docker push ghcr.io/thenervelab/hub-blob-receiver:<tag>
```

Only the receiver binary is built (`cargo build -p hub-blob-receiver`); the
hippius_core pyo3 crate is not a dependency and is skipped, so no Python
toolchain is needed.

## Deploy

```bash
kubectl apply -k deploy/receiver/    # deployment + service + PDB
```

These manifests are authored here and consumed by the infra/GitOps repo. Set the
image tag and `HARBOR_BASE` per environment (Kustomize overlay or GitOps patch).

## Configuration (env)

| Var           | Default                                  | Purpose |
|---------------|------------------------------------------|---------|
| `HARBOR_BASE` | `https://registry.hippius.com`           | Harbor base URL for the LAN push. In-cluster, point at the Harbor Service. |
| `SCRATCH_DIR` | `/scratch`                               | NVMe staging dir (the `emptyDir` mount). |
| `BIND`        | `0.0.0.0:8080`                           | Listen address. |
| `RUST_LOG`    | `info`                                   | `tracing` filter. |

## Client wiring

The client routes to the receiver only when `HIPPIUS_RECEIVER_URL` points at
this Service and the blob clears `HIPPIUS_MULTIPART_THRESHOLD` (default 256 MB).
With `HIPPIUS_RECEIVER_URL` unset, uploads are unchanged (single streaming PUT).

## Scratch sizing

The `emptyDir` `sizeLimit` must cover `maxConcurrentUploads × N × partSize` of
in-flight staged bytes. Default 64 Gi is a starting point — set it from the
observed concurrency once the diagnose measurement (below) is run.

## Durability caveat (read before relying on it)

Session state is in memory and parts live on the ephemeral `emptyDir`. A pod
restart mid-upload loses both, so the client must restart that blob (it gets
`404 UnknownUpload`). The PDB (`minAvailable: 2`) bounds how many in-flight
uploads a rollout can disrupt; a durable fix (persist session metadata so a
fresh pod rehydrates) is tracked as post-v1 hardening.

Because sessions are per pod, the Service uses `sessionAffinity: ClientIP` so a
client's `initiate` → parts → `complete` all reach the same pod. **Do not remove
that affinity while sessions are in memory** — round-robin would send part PUTs
to pods that never saw the session (`404 UnknownUpload`). Note ClientIP affinity
pins by source IP, so many clients behind one egress IP share a pod; that trades
load balancing for correctness until the shared-store fix lands.

Auth is passed through: the receiver replays the client's bearer token to Harbor
and holds no credential of its own.

## Deployment gate (not yet run)

Roll out only after the in-cluster diagnose pass confirms the single-stream
ingest ceiling into Harbor is at or above the N-way WAN aggregate a fast client
delivers — otherwise the receiver's one LAN stream becomes the new bottleneck.
See the plan's §"Deployment gate".
