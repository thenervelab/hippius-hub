# Prod runbook: Harbor filesystem (JuiceFS) → S3, same numbers as staging

**Nothing in this document has been applied to prod.** Prod Harbor is still Helm revision 4, filesystem on JuiceFS (re-verified 2026-08-26).

One change at the helm flip: where `harbor-registry` stores blob bytes. Hostname, chart, Postgres, robots, docker stay. `hippius-hub` 0.7.0 is enough for the 80 bar; the config-blob overlap in [PR #89](https://github.com/thenervelab/hippius-hub/pull/89) is extra.

Artifacts: `deploy/harbor-s3-prod/` (flip) and `deploy/harbor-staging/` (isolated gate).

---

## Same numbers as staging (read this first)

Unique 1 GiB on **harbor-staging → gateway.hippius-s3-staging**, stock `hippius_hub==0.7.0`, 3 runs:

| Client | Median | Runs |
|---|---|---|
| Stock 0.7.0 | **99.6 MiB/s** (99.6 / 86.0 / 116.2) | PASS vs 80 |
| Harbor → MinIO (Harbor-itself ceiling) | 159.3 MiB/s | not this path |

Config-blob overlap ([#89](https://github.com/thenervelab/hippius-hub/pull/89)) is extra, not a gate, and **only applies to `upload_file`**. `upload_folder` still PUTs the empty `{}` config blob synchronously in `_finalize_upload_manifest` after the file fan-out; folder uploads get none of this overlap. Unique 1 GiB **n=5**, `upload_file`, same Harbor knobs (2026-08-26): stock 0.7.0 median **96.8 MiB/s** (82.8 / 89.1 / 96.8 / 97.5 / 100.3); overlap overlay median **122.5 MiB/s** (107.9 / 116.9 / 122.5 / 126.7 / 144.3). 122.5 / 96.8 ≈ **1.27× is that single-file number**, not a general hub win. Ranges do not overlap; spread is still large; do not quote a single 134 MiB/s figure. The process-wide config cache only helps the first upload to a repo.

That 99.6 is **not** “switch Harbor to S3 and you’re done.” The first hippius-s3 gate the same day, against a gateway without alias CopyObject, was **21.3 MiB/s**. Prod hippius-s3 is still that older image.

| Layer | Staging (the 99.6 path) | Prod today (2026-08-25) |
|---|---|---|
| hippius-s3 api | `api:19bcdb1` (staging) | `api:539eec1` (main, merge of #413) |
| CopyObject | `#445` object_names alias (~0.10 s / 657 MiB/s for 64 MiB) | streaming GET+PUT (**4.3 MiB/s**) |
| Fresh-part GET | `#448` + `#451` cipher sizes on the Redis hint | missing — 5–15 s startedat GET tails |
| `UVICORN_WORKERS` / `API_DB_POOL_MAX_SIZE` | 8 / 8 (2 api-local; 128 < PG 200) | 4 / 15 (5 api-local; PG `max_connections=1000`) |
| Harbor storage | S3 driver, ns `harbor-staging`, helm rev 4 + kubectl knobs | filesystem / JuiceFS, ns `harbor`, helm rev 4 |
| Harbor `multipartcopythresholdsize` | **live 128 MiB** (kubectl patch). Overlay YAML is **5 GiB** for blobs >128 MiB. Unique 1 GiB packs are 64–75 MiB, so 128 MiB vs 5 GiB is the same alias path for the bar. | n/a (filesystem) |
| Harbor `chunksize` | 64 MiB | n/a |
| Registry | **3** replicas × CPU 2/4 | **1** replica, BestEffort (overlay sets 2/4, keeps 1 replica) |
| Core / nginx | 3 / 3 with CPU requests | **5 / 5**, BestEffort (overlay sets CPU, keeps 5) |
| Client | stock 0.7.0 | stock 0.7.0 |

Flipping Harbor onto **today’s** prod gateway reproduces 21.3, not 99.6.

Order:

1. **Promote hippius-s3 staging → prod** (`19bcdb1` = `#445` + `#448` + `#451`, and workers — see §A). Do not helm `-n harbor` before this.
2. Re-run the S3 contract against `gateway.hippius-s3-prod`. CopyObject 64 MiB must be ≥ 50 MiB/s (alias). The contract script now **FAIL**s below that.
3. Copy existing blobs. Freeze pushes for minutes. Helm `-n harbor` with `overlay-s3.yaml` (must keep `multipartcopythresholdsize=5368709120`).
4. Prove unique 1 GiB stock 0.7.0 median ≥ 80 MiB/s.

---

## Decision for the team

| # | Decision | Proposed | Why |
|---|---|---|---|
| D1 | Keep Harbor 2.15. Switch only `persistence.imageChartStorage.type` to `s3`. | **Yes** | Staging unique 1 GiB 99.6 vs prod JuiceFS 47. |
| D2 | Copy blobs to a **new** bucket using Harbor’s own keys. Same Postgres. No `pgdump`. | **Yes** | Blobs 404 if missing. Manifests/repos live in Postgres. |
| D3 | `disableredirect: true` on day one (no 307 → presigned GET). | **Yes** | ATS cache key includes SigV4 query string. Redirect is a later change. |
| D4 | Freeze = Harbor `read_only` for **minutes** at the helm flip, after copy has caught up. | **Yes** | Freeze-before-copy = hours of downtime. |
| D5 | Keep PVC `harbor-registry` until a later soak. | **Yes** | Rollback path. |
| D6 | No JuiceFS format, no Harbor replication endpoints, no new registry. Client: stock 0.7.0 is enough for 80. [#89](https://github.com/thenervelab/hippius-hub/pull/89) is optional extra. | **Yes** | Measured. |
| D7 | Pass the **hippius-s3** gate (not MinIO) on the **prod** gateway after §A, before copy. | **Yes** | Staging 99.6 used `gateway.hippius-s3-staging`. Prod writes `gateway.hippius-s3-prod`. |
| D8 | Promote hippius-s3 `#445`/`#448`/`#451` + worker overlay to prod **before** the Harbor helm flip. | **Yes** | Without this, Harbor Move is 4.3 MiB/s CopyObject and unique 1 GiB is ~21. |

Sign-off needed on D1–D8, then fill §0. Infra runs helm in ns `harbor`. George does not.

Success bar after flip: median **≥ 80 MiB/s** on a 1 GiB fresh unique-bytes `hippius-hub` 0.7.0 `upload_file` (3 runs). Do not treat staging n=5 **96.8 / 122.5** as the prod target: those were harbor-staging with **3** registry replicas. Prod overlay keeps **1** registry replica. Bar is 80. Do not quote 134. Folder uploads unchanged.

---

## Live prod (do not change until §6)

```text
cluster:     hippius (rancher c-m-ff5bhnvp)
kubeconfig:  ~/Hippius-Storage/Configs/k8s/hippius.yaml
release:     harbor
namespace:   harbor
revision:    4   (2026-05-08)   chart harbor-1.19.0 / app 2.15.0
externalURL: https://registry.hippius.com
Service:     NodePort 80:30002
postgres:    external harbor-postgres-rw.harbor.svc.cluster.local  (keep)
registry:    deploy/harbor-registry  1 replica  BestEffort  pod on k8s-v3-node1
core/nginx:  5 / 5  BestEffort
jobservice:  1 replica  RWO ceph-block  pod on k8s-v3-node4
redis:       statefulset/harbor-redis  pod harbor-redis-0  (not a Deployment)
             registry config.yml redis.addr=harbor-redis:6379  db: 2
storage:     filesystem  rootdirectory=/storage  cache.layerinfo: redis
PVC:         harbor-registry → pv pvc-b0dac713-…  100Ti RWX  hippius-juicefs
             annotation helm.sh/resource-policy: keep   (present, re-checked 2026-08-26)
S3 gateway:  http://gateway.hippius-s3-prod.svc.cluster.local:8080
s3 image:    ghcr.io/thenervelab/hippius-s3/api:539eec1
s3 workers:  UVICORN_WORKERS=4  API_DB_POOL_MAX_SIZE=15  (5 api-local)
s3 postgres: max_connections=1000
harbor-s3 secret: not created yet (correct)
```

On disk (copy both trees; skip `_uploads`):

```text
/storage/docker/registry/v2/blobs/sha256/<aa>/<64-hex>/data
/storage/docker/registry/v2/repositories/<name>/_manifests/.../link
/storage/docker/registry/v2/repositories/<name>/_layers/sha256/<digest>/link
```

S3 keys after copy (`rootdirectory` omitted): the same paths under `s3://<bucket>/docker/registry/v2/`.

Postgres holds Harbor's API catalog (projects, members, artifact rows). The **registry** resolves tags via `repositories/**/link` files. Copying only `blobs/` leaves `docker pull` with no tag. Link files are named `link` (~71 bytes) and tag `current/link` is mutable — never `--size-only` on that pass.

Do not copy `_uploads/` (in-flight sessions). Do not copy JuiceFS internal chunks. Blob `data` files are ~1.9 TB (July census), **not** JuiceFS `used_space` 31 TiB.

`helm get values harbor -n harbor` (user-supplied) does **not** contain the node1 affinity. That is a live kubectl patch. A helm upgrade without the overlay affinity **drops** it (already happened once).

User-supplied values also contain DB and admin passwords. **Never** `helm get values` into git or Slack. Use `--reuse-values`.

---

## What we are not doing

- New Harbor, new namespace, CAS, `pgdump`/`pgrestore`
- Harbor replication endpoints
- JuiceFS `--block-size` / `format --force`
- A hub release as a gate (0.7.0 already ≥ 80 on the staging path)
- GET 307 / redirect
- Deleting PVC `harbor-registry`
- `helm uninstall harbor`
- Writing into bucket `hippius-juicefs-data`
- Helm-upgrading ns `harbor` before §A (hippius-s3 promote)

---

## 0. Fill before any apply

| # | Item | Owner | Notes |
|---|---|---|---|
| 0.1 | Bucket name, **not** `hippius-juicefs-data` | s3 | Suggested: `harbor-registry-cas`. Replace `REPLACE_ME_BUCKET` in the YAML files. |
| 0.2 | `hip_` key scoped to that bucket | s3 | Secret `harbor-s3` in ns `harbor`, keys `REGISTRY_STORAGE_S3_ACCESSKEY` / `REGISTRY_STORAGE_S3_SECRETKEY` |
| 0.3 | That Arion account has credits | s3 | `can_upload` 402 freezes writes (hit 2026-08-23) |
| 0.4 | Who runs `helm upgrade -n harbor --version 1.19.0` | infra | George does not |
| 0.5 | Chart `harbor/harbor` **1.19.0** still pullable | infra | `helm pull harbor/harbor --version 1.19.0`. Do not upgrade the app. |
| 0.6 | Who promotes hippius-s3 staging → prod | s3 | Image `19bcdb1` (or later staging that still contains #445/#448/#451) |

Helm repo `harbor` → `https://helm.goharbor.io` is already on this machine.

Do not put the Harbor S3 secret in namespace `harbor` until §A and §1 pass.

---

## A. Promote hippius-s3 to the staging image (required before Harbor helm)

Prod today: `api:539eec1`, `UVICORN_WORKERS=4`. Staging: `api:19bcdb1`, workers 8 / pool 8.

Must be on prod gateway before Harbor writes to it:

- [thenervelab/hippius-s3#445](https://github.com/thenervelab/hippius-s3/pull/445) — same-bucket CopyObject = extra `object_names` row on the source `object_id`. Do **not** re-enable the v5 CID-reuse fast path (AAD is `bucket_id+object_id`).
- [thenervelab/hippius-s3#448](https://github.com/thenervelab/hippius-s3/pull/448) — Redis ingest-node hint so a just-written part is not a 5–15 s GET 206.
- [thenervelab/hippius-s3#451](https://github.com/thenervelab/hippius-s3/pull/451) — cipher sizes on that hint.
- `UVICORN_WORKERS=8`. Staging also set `API_DB_POOL_MAX_SIZE=8` because staging PG is 200 and there are 2 api-local pods (`8*8*2=128`). Prod has **5** api-local and PG `max_connections=1000`. Do not copy 8/15: `8*15*5=600` plus other pools. `8*8*5=320` fits. Keep pool at 8 when workers go to 8.

This promote is a hippius-s3 deploy, not a Harbor helm. After it:

```bash
export KUBECONFIG=~/Hippius-Storage/Configs/k8s/hippius.yaml
kubectl -n hippius-s3-prod get ds api-local -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
# expect: .../api:19bcdb1  (or a later sha that contains 445/448/451)

# workers from the live env (defaults CM is 4/15 until the overlay is live)
kubectl -n hippius-s3-prod get cm hippius-s3-environment -o yaml
```

If the image is still `539eec1`, **stop**. Do not copy blobs. Do not helm `-n harbor`.

---

## 1. hippius-s3 contract on the **prod** gateway (required, not MinIO)

Arm C was Harbor → MinIO. Staging unique 1 GiB 99.6 was Harbor → `gateway.hippius-s3-staging`. This step is Harbor’s S3 ops against **`http://gateway.hippius-s3-prod.svc.cluster.local:8080`** after §A.

**Do not run `./deploy/harbor-staging/install-hippius-s3.sh` against the prod Harbor bucket (0.1).** That script helms Harbor in `harbor-staging` onto whatever bucket you pass. harbor-staging is already up (helm rev 4, 2026-08-25) and its jobservice is RWO — a re-helm Multi-Attaches. Pointing it at 0.1 would also write a second Harbor’s `docker/registry/v2/` tree into the bucket the copy job is about to fill.

After §A, run **only** the contract Job against the prod gateway. The contract writes `harbor-s3-probe/<run-id>/` and deletes it. Unique 1 GiB on **prod Harbor** is §7, after the flip.

```bash
export KUBECONFIG=~/Hippius-Storage/Configs/k8s/hippius.yaml
export HARBOR_S3_BUCKET=<0.1>
export HARBOR_S3_ACCESS_KEY=hip_...
export HARBOR_S3_SECRET_KEY=...
export HARBOR_S3_ENDPOINT=http://gateway.hippius-s3-prod.svc.cluster.local:8080

# Reuse the existing harbor-staging namespace for the Job only.
# The Job YAML reads secret harbor-staging-s3 (HARBOR_S3_BUCKET / keys / endpoint).
kubectl -n harbor-staging create secret generic harbor-staging-s3 \
  --from-literal=REGISTRY_STORAGE_S3_ACCESSKEY="${HARBOR_S3_ACCESS_KEY}" \
  --from-literal=REGISTRY_STORAGE_S3_SECRETKEY="${HARBOR_S3_SECRET_KEY}" \
  --from-literal=HARBOR_S3_BUCKET="${HARBOR_S3_BUCKET}" \
  --from-literal=HARBOR_S3_ENDPOINT="${HARBOR_S3_ENDPOINT}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n harbor-staging create configmap hippius-s3-contract \
  --from-file=hippius_s3_contract.py=deploy/harbor-s3-prod/hippius_s3_contract.py \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n harbor-staging delete job hippius-s3-contract --ignore-not-found
kubectl apply -f deploy/harbor-s3-prod/hippius-s3-contract-job.yaml
kubectl -n harbor-staging wait --for=condition=complete job/hippius-s3-contract --timeout=20m
kubectl -n harbor-staging logs job/hippius-s3-contract
```

Contract (`deploy/harbor-s3-prod/hippius_s3_contract.py`): path-style SigV4, tiny PUT, 64 MiB PUT, Range GET, MPU, CopyObject (Harbor blob-commit Move), List, Delete. 402 credits = fail. **CopyObject 64 MiB < 50 MiB/s = FAIL** (streaming, not alias). If it fails, **stop**. Do not copy. Do not helm `-n harbor`.

Do not treat the MinIO 93 / 159 MiB/s numbers as this gate. Do not helm `harbor-staging` again.

---

## 2. Pre-flight (read-only + secret only)

```bash
export KUBECONFIG=~/Hippius-Storage/Configs/k8s/hippius.yaml

kubectl -n harbor get deploy harbor-registry
kubectl -n harbor get pvc harbor-registry
kubectl -n harbor get cm harbor-registry -o jsonpath='{.data.config\.yml}' | head -20
# must still show storage.filesystem.rootdirectory=/storage

# PVC must keep this or helm will delete it when type becomes s3:
kubectl -n harbor get pvc harbor-registry -o jsonpath='{.metadata.annotations.helm\.sh/resource-policy}{"\n"}'
# expect: keep
# if empty:
# kubectl -n harbor annotate pvc harbor-registry helm.sh/resource-policy=keep
```

After §1 passes, create the secret in ns `harbor` (after 0.2). File: `deploy/harbor-s3-prod/secret-harbor-s3.yaml`.

```bash
kubectl -n harbor apply -f deploy/harbor-s3-prod/secret-harbor-s3.yaml
```

Replace `REPLACE_ME_BUCKET` in:

- `deploy/harbor-s3-prod/overlay-s3.yaml`
- `deploy/harbor-s3-prod/copy-blobs-job.yaml`

---

## 3. Helm dry-run (no freeze)

```bash
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values \
  -f deploy/harbor-s3-prod/overlay-s3.yaml \
  --dry-run --debug
```

Inspect the rendered `harbor-registry` ConfigMap `config.yml`:

| Must see | Must not see |
|---|---|
| `storage.s3.bucket: <0.1>` | `bucket: bucketname` |
| `regionendpoint: http://gateway.hippius-s3-prod.svc.cluster.local:8080` | `storage.filesystem` |
| `chunksize: 67108864` | NodePort / postgres / `externalURL` rewritten |
| `multipartcopythresholdsize: 5368709120` | affinity dropped |
| `redirect.disable: true` | PVC `harbor-registry` **deleted** |
| env `REGISTRY_STORAGE_S3_FORCEPATHSTYLE=true` | chart version ≠ 1.19.0 |
| env `REGISTRY_STORAGE_S3_SECURE=false` | |
| env `REGISTRY_STORAGE_S3_MULTIPARTCOPYTHRESHOLDSIZE=5368709120` | `bucket: REPLACE_ME_BUCKET` |

Default `multipartcopythresholdsize` is **32 MiB**. A 64–75 MiB pack then Move’s via `UploadPartCopy` of 32 MiB ranges (GET+PUT) instead of one CopyObject alias. That alone keeps unique 1 GiB at ~79 instead of ~100.

`existingClaim: harbor-registry` plus `resourcePolicy: keep` plus the live annotation is what stops Helm from removing the JuiceFS PVC when `type` is no longer `filesystem` (chart only emits that PVC for filesystem).

Chart 1.19.0 does not render `s3.forcepathstyle`, and it omits `s3.secure=false`, so those two **must** stay as extraEnvVars. Do not bump the chart to “fix” that.

If dry-run wants to delete the PVC, rewrite NodePort 30002, touch postgres, bump the chart, or still has `REPLACE_ME_BUCKET`: **stop**.

`--reuse-values` plus this overlay also sets CPU on `harbor-core` (5 replicas) and `harbor-nginx` (5 replicas). Those roll. Pull 502 is the **1** registry replica; core/nginx rollingUpdate should keep some up.

`harbor-jobservice` is 1 replica on a **RWO** `ceph-block` PVC (today on `k8s-v3-node4`). Staging already wedged helm on that Multi-Attach. Scale it to 0 before the real upgrade (§6), not only in the dry-run.

---

## 4. Online copy (Harbor stays serving)

Job mounts PVC `harbor-registry` read-only (RWX). Registry pod is not restarted.

```bash
kubectl -n harbor apply -f deploy/harbor-s3-prod/copy-blobs-job.yaml
kubectl -n harbor logs -f job/harbor-blob-copy
```

Two rclone passes in the Job (do not collapse them):

```text
blobs/**/data            → s3://…/blobs/…     --size-only (content-addressed)
repositories/**/link     → s3://…/repositories/…   --checksum, NOT --size-only
                           exclude _uploads/
```

`--checksum` on links is load-bearing for catch-up. Default rclone compare is size+modtime; S3 object mtime is the copy time, so without `--checksum` every 71-byte `link` recopies on every re-run and “~0 bytes” never happens. With `--checksum`, unchanged tag/layer links skip; a tag that moved recopies.

Re-run the Job (delete + apply) until **both** passes transfer ~0 bytes (blobs size-match; links checksum-match). A blobs-only copy is a failed cutover: §7 `docker pull` of a known tag 404s.

Before copy, confirm the target bucket has **no** lifecycle expiration/transition:

```bash
# ClusterIP — run from a pod in the cluster, not from a laptop.
# must be NoSuchLifecycleConfiguration / empty. A rule that expires or
# tiers objects deletes blobs the Harbor DB still references.
aws --endpoint-url http://gateway.hippius-s3-prod.svc.cluster.local:8080 \
  s3api get-bucket-lifecycle-configuration --bucket <0.1>
```

Retry on 503 / SlowDown is in rclone flags. Do not point rclone at `hippius-juicefs-data`.

Sample check: SHA-256 of local `…/blobs/sha256/<aa>/<digest>/data` equals `<digest>`; S3 `HEAD` size equals file size (≥20 random blobs including one large pack). Plus: one tag `current/link` bytes on disk equal the S3 object (not merely the same 71-byte size).

Expected wall clock: hours, not minutes (~1.9 TB). That is the long pole. **Do not freeze yet.**

Optional size check before copy:

```bash
kubectl -n harbor run harbor-blob-du --rm -it --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"du","image":"busybox:1.37","command":["sh","-c","du -sh /storage/docker/registry/v2/blobs; find /storage/docker/registry/v2/blobs -type f -name data | wc -l"],"volumeMounts":[{"name":"r","mountPath":"/storage","readOnly":true}]}],"volumes":[{"name":"r","persistentVolumeClaim":{"claimName":"harbor-registry"}}]}}' \
  --image=busybox:1.37
```

---

## 5. Freeze (minutes)

Only when §4 catch-up is idle.

```bash
ADMIN=$(kubectl -n harbor get secret harbor-core \
  -o jsonpath='{.data.HARBOR_ADMIN_PASSWORD}' | base64 -d)

# freeze pushes (pulls stay up until the registry restart).
# -f: Harbor 2.x CSRF/403 must not be ignored.
curl -sS -f -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":true}'

curl -sS -f -u "admin:${ADMIN}" \
  https://registry.hippius.com/api/v2.0/configurations \
  | python3 -c "import json,sys; c=json.load(sys.stdin); assert c.get('read_only') is True, c; print('read_only', c['read_only'])"
```

1. Announce: no pushes.
2. Wait in-flight uploads to finish or fail (they retry after).
3. Delete + re-apply `copy-blobs-job.yaml` once more (should be seconds–minutes).
4. Go to §6 immediately. **Do not unfreeze.** Unique 1 GiB is an upload; it cannot run while `read_only`. Pull-prove first (§7a), then unfreeze, then the upload bar.

---

## 6. Flip (the only prod Helm upgrade)

`harbor-jobservice` PVC is RWO `ceph-block` (live pod on `k8s-v3-node4`). A helm upgrade restarts it and staging already wedged on Multi-Attach. Scale it to 0 first. Trivy is also RWO; if helm hangs on `harbor-trivy-0`, wait for the volume detach and delete the stuck pod.

```bash
kubectl -n harbor scale deploy/harbor-jobservice --replicas=0
kubectl -n harbor rollout status deploy/harbor-jobservice --timeout=2m

helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values \
  -f deploy/harbor-s3-prod/overlay-s3.yaml
```

Pushes **and** pulls 502 while `harbor-registry` (1 replica) restarts. Core and nginx also roll (overlay sets CPU); they have 5 replicas so some stay up. Postgres is not restarted.

```bash
kubectl -n harbor rollout status deploy/harbor-registry --timeout=5m
kubectl -n harbor rollout status deploy/harbor-core --timeout=5m
kubectl -n harbor rollout status deploy/harbor-nginx --timeout=5m

kubectl -n harbor scale deploy/harbor-jobservice --replicas=1
kubectl -n harbor rollout status deploy/harbor-jobservice --timeout=5m

kubectl -n harbor get cm harbor-registry -o jsonpath='{.data.config\.yml}'
# must show storage.s3, chunksize 67108864, multipartcopythresholdsize 5368709120
# must not show storage.filesystem

kubectl -n harbor get deploy harbor-registry -o jsonpath='{.spec.template.spec.affinity}{"\n"}'
# must still prefer k8s-v3-node1

kubectl -n harbor get pvc harbor-registry
# must still be Bound

kubectl -n harbor get deploy harbor-registry -o json
# env must include FORCEPATHSTYLE=true, SECURE=false,
# MULTIPARTCOPYTHRESHOLDSIZE=5368709120
```

Flush the registry blobdescriptor cache so leftover filesystem `layerinfo` entries do not 404 after the driver switch. Live `config.yml` is `redis.addr: harbor-redis:6379` / `db: 2`. Redis is **StatefulSet** `harbor-redis`, pod `harbor-redis-0` — not a Deployment. Do **not** FLUSHALL (Harbor core uses db 0). Do **not** FLUSHDB 0.

```bash
REDIS_DB=$(kubectl -n harbor get cm harbor-registry -o jsonpath='{.data.config\.yml}' \
  | awk '/^redis:/{r=1} r && /db:/{print $2; exit}')
if [[ -z "${REDIS_DB}" || "${REDIS_DB}" == "0" ]]; then
  echo "refusing FLUSHDB: parsed db=${REDIS_DB:-empty} (core is 0)" >&2
  exit 1
fi
kubectl -n harbor exec harbor-redis-0 -- redis-cli -n "${REDIS_DB}" FLUSHDB
```

**Stay `read_only`.** Unfreeze is §7b, after pull-prove.

---

## 7. Prove (before anyone calls it done)

Unique 1 GiB is an **upload**. It cannot run while `read_only`. Split the prove.

### 7a. While still `read_only` (rollback is cheap)

From the same kind of box as arm B if possible (`test/e2e-client` / `benchmark.yml`):

1. Download of a **copied** public model that was **not** re-uploaded (proves keys).
2. `docker pull` of a known tag.
3. Existing `robot$…` login (no rotate).
4. Anonymous pull of a public project.
5. `hippius-hub registry me` / provision still talk to the same Harbor API.

If (1) or (2) 404s: keys or prefix are wrong — roll back (§8) **now**, still frozen, no reverse copy.

### 7b. Unfreeze

```bash
curl -sS -f -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":false}'
```

### 7c. Upload bar (writes land on S3 only)

`hippius-hub` 0.7.0, 1 GiB **fresh unique** bytes, 3 runs. Median **≥ 80 MiB/s**. Repeating an 8 MiB buffer is a FastCDC stampede, not this bar. This is `upload_file`, not `upload_folder`.

If (7c) is ~20 MiB/s: hippius-s3 CopyObject is still streaming — §A did not land. Freeze again and roll back (§8). Blobs pushed during 7c are **not** on JuiceFS.

If (7c) is ~45–80: Harbor `multipartcopythresholdsize` is still 32 MiB (Move = `UploadPartCopy` slices). Check the live `config.yml` and extraEnv. Do not helm again without `--reuse-values` and the overlay.

Prod is 1 registry replica; do not expect staging’s 96.8. Bar is 80.

---

## 8. Rollback (minutes)

JuiceFS PVC is still Bound and still has pre-flip bytes.

```bash
# freeze again if you already unfroze
curl -sS -f -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":true}'

kubectl -n harbor scale deploy/harbor-jobservice --replicas=0
kubectl -n harbor rollout status deploy/harbor-jobservice --timeout=2m

helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values \
  -f deploy/harbor-s3-prod/overlay-filesystem-rollback.yaml

kubectl -n harbor rollout status deploy/harbor-registry --timeout=5m
# config.yml must show filesystem /storage again
# PVC still Bound; affinity still node1

# Drop S3-era layerinfo so pulls do not 404 on filesystem.
REDIS_DB=$(kubectl -n harbor get cm harbor-registry -o jsonpath='{.data.config\.yml}' \
  | awk '/^redis:/{r=1} r && /db:/{print $2; exit}')
if [[ -z "${REDIS_DB}" || "${REDIS_DB}" == "0" ]]; then
  echo "refusing FLUSHDB: parsed db=${REDIS_DB:-empty}" >&2
  exit 1
fi
kubectl -n harbor exec harbor-redis-0 -- redis-cli -n "${REDIS_DB}" FLUSHDB

kubectl -n harbor scale deploy/harbor-jobservice --replicas=1

curl -sS -f -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":false}'
```

Blobs **pushed during the S3 window** are not on JuiceFS. Rollback after a long S3 write window without a reverse copy **drops those blobs**. That is why prove-before-unfreeze matters.

Do not delete the PVC. Do not `juicefs format --force`.

---

## 9. After soak (not this change)

~7 days of S3 as source of truth: then you may stop relying on the JuiceFS PVC. Separate change.

Optional later: merge/release [hub #89](https://github.com/thenervelab/hippius-hub/pull/89) (config-blob overlap on `upload_file` only). Not a gate. Do not quote 134.

---

## Owners

| Step | Who |
|---|---|
| 0.1–0.3 bucket + key + credits | s3 |
| 0.6 + §A hippius-s3 promote | s3 |
| 1 hippius-s3 gate (contract Job on prod gateway, **not** a harbor-staging re-helm) | George |
| 0.4–0.5, 3, 6, 8 helm (scale jobservice to 0 first on 6 and 8) | infra |
| 4 copy Job | whoever can `kubectl apply` a Job that mounts `harbor-registry` |
| 5 freeze; 7b unfreeze | George + infra |
| 7 prod prove (7a pulls frozen, 7c upload after unfreeze) | George (`benchmark.yml` / `test/e2e-client`) |

---

## Stop-the-line

- hippius-s3-prod still on `api:539eec1` (no #445 alias)
- Contract CopyObject 64 MiB < 50 MiB/s
- Dry-run changes NodePort, postgres, `externalURL`, or chart version
- Dry-run deletes PVC `harbor-registry`
- Dry-run drops `multipartcopythresholdsize` or node1 affinity
- Copy destination is `hippius-juicefs-data`
- `helm upgrade` without `--reuse-values` or without `--version 1.19.0`
- `helm upgrade` without the node1 affinity overlay
- Dump/restore of `harbor-postgres`
- `helm uninstall harbor`
- Any helm command in a namespace other than `harbor` except isolated `harbor-staging`
- Enabling redirect / 307 on this flip
- Skipping §A / §1 (MinIO arm C is not this gate)
- Bucket or endpoint containing `juicefs` or `minio`
- Unique-1GiB prove with a repeating 8 MiB buffer
- Copy job that only syncs `blobs/` (no `repositories/**/link`) — `docker pull` will not resolve tags
- `--size-only` on the repositories/link pass (tag `current/link` is 71 bytes and mutable)
- Skipping the Redis `layerinfo` FLUSHDB after the driver switch
- Live `config.yml` missing `multipartcopythresholdsize: 5368709120` after helm (Move falls back to 32 MiB UploadPartCopy)
- Setting `multipartcopythresholdsize` **above** 5368709120 — S3 caps a single-operation `CopyObject` at 5 GiB, so distribution would attempt a simple copy the gateway must reject
- Running Harbor **GC** against a partially-copied bucket. GC trusts the Harbor DB, not the backend, so it deletes blobs it cannot see (distribution [#19308](https://github.com/distribution/distribution/issues/19308)). No GC until the JuiceFS PVC is deleted
- Pointing Harbor at a bucket with an **S3 lifecycle rule**. Audit the target bucket for expiration/transition rules before the flip — a rule that expires or tiers objects silently deletes blobs the DB still references
- Helming `harbor-staging` onto the **prod Harbor bucket** (0.1) — a second registry tree in the same prefix
- `kubectl exec deploy/harbor-redis` — Redis is `pod/harbor-redis-0`
- `FLUSHDB` on db 0 (Harbor core) or `FLUSHALL`
- Unfreezing before pull-prove (7a). Unique 1 GiB cannot run while `read_only`; do pulls first
- Helm upgrade while `harbor-jobservice` is 1/1 on RWO `ceph-block` (scale to 0 first; staging already Multi-Attached)
- Overlay still containing `REPLACE_ME_BUCKET`
- Treating staging unique 1 GiB 96.8 (3 registry replicas, 128 MiB threshold) as the prod number (1 replica)
