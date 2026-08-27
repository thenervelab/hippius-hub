# Prod runbook: Harbor storage driver filesystem → s3

Team evaluation copy. **Nothing in this document has been applied.** Prod is still Helm revision 4, filesystem on JuiceFS (verified 2026-08-25).

One change: where `harbor-registry` stores blob bytes. Hostname, chart, Postgres, robots, docker, and `hippius-hub` stay.

Artifacts: `deploy/harbor-s3-prod/`.

---

## Decision for the team

| # | Decision | Proposed | Why |
|---|---|---|---|
| D1 | Keep Harbor 2.15. Switch only `persistence.imageChartStorage.type` to `s3`. | **Yes** | Arm C: 93 MiB/s vs prod JuiceFS 47 MiB/s. Client already 769 MiB/s. |
| D2 | Copy blobs **and rebuild the per-repository link objects** into a **new** bucket using Harbor’s own keys. Same Postgres. No `pgdump`. | **Yes** | Blobs 404 if missing. Tags live in Postgres, but `_layers` / `_manifests/revisions` links do **not** — see §4a. |
| D3 | `disableredirect: true` on day one (no 307 → presigned GET). | **Yes** | ATS cache key includes SigV4 query string. Redirect is a later change. |
| D4 | Freeze = Harbor `read_only` for **minutes** at the helm flip, after copy has caught up. | **Yes** | Freeze-before-copy = hours of downtime. |
| D5 | Keep PVC `harbor-registry` until a later soak. | **Yes** | Rollback path. |
| D6 | No client release, no JuiceFS format, no Harbor replication endpoints, no new registry. | **Yes** | Stock `hippius_hub==0.7.0` unique 1 GiB median **99.6 MiB/s** on harbor-staging after Harbor `multipartcopythresholdsize=134217728`. Config-blob overlap is extra (134.1), not required for the bar. Replication re-examined 2026-08-27 — reasoning below. |
| D7 | Pass the **hippius-s3** gate (not MinIO) before copy. | **Yes** | Arm C 93 MiB/s was MinIO. Prod writes hippius-s3. |
| D8 | Prove reachability for **every** artifact before the unfreeze, not a sample. | **Yes** | Both the copy and any alternative fail the same way: some artifacts resolve, some 404, silently. §7a is ~56k HEADs and no bytes. |

**Why not replication (D6, re-checked against the 2.15 docs 2026-08-27).** Harbor's
endpoint providers are all *registries* — ECR, ACR, GCR, GHCR, Docker Hub, Docker
registry, Artifactory, SWR, TCR, VolcEngine, Alibaba, plus other Harbor instances.
There is no object-storage target, so replication cannot populate the bucket for an
in-place driver flip.

It would allow a different shape — a second S3-backed Harbor, replicate, cut the
hostname — which would make Harbor write its own link objects. We are not doing that
because replication moves artifacts, not the deployment:

- **351 active project robots** would not come across. Secrets are stored hashed and
  the CLI hands them out once at `registry provision`; a new Harbor cannot reissue the
  same secret, so every user `docker login` and CI credential breaks until Console
  re-provisions all 237 projects.
- **1,310 untagged, unreferenced artifacts** would be dropped — rules filter on tag and
  `artifact_reference` holds only 5 rows, so these are standalone. They are recent (902
  pushed in July, 408 in August) and `hippius-hub` reaches them by digest
  (`_oci.py:76` passes `revision` straight through).
- 237 quotas, 239 project memberships, 251 CVE allowlists and the 217/20 public-private
  split do not replicate either.
- It reads back through the source registry — the JuiceFS path that is the current
  bottleneck — where rclone reads the PVC directly.

Replication is still worth using as a **rehearsal**: a Harbor-to-Harbor rule into the
S3-backed `harbor-staging` makes Harbor generate the link objects itself, giving a
reference set to diff §4a's output against without touching prod storage.

**"Mirror mode" — evaluated 2026-08-27 against the 2.15 docs, and rejected.** In Harbor's
vocabulary a mirror is a *proxy cache project*, and the docs are explicit: "you are not
able to push images to a proxy cache project." Hub users push constantly, so a
proxy-cache-backed registry is read-only and unusable as primary. It is also lazily
populated — only what someone pulls ever lands, so the cold majority of 28k artifacts
would never migrate — and Harbor "creates a 7 day retention policy for each new proxy
cache project" by default, so what does land is evicted. It is a cache, not a store.
(goharbor #22611, targeted 2.15.0, argues even the "pull-through cache" label is wrong.)

**What the research does confirm is §4a.** Harbor has no migration tooling — goharbor
#18843, asking for exactly this, was closed *as not planned* — and the community guidance
is unambiguous: copy the entire `docker/registry/v2/` tree, blobs *and* repositories,
skipping `_uploads/`. Without the link files "the registry has no record that a repository
owns a given layer, so pulls will fail with blob-unknown errors". Two Harbor issues are
this failure in the wild: #17541 (swapped S3 buckets, "docker login worked but push/pull
broken, no obvious indicators in the logs", closed with no maintainer answer) and #11773
(rebuilt Harbor on the same S3 showed no images). GitLab hit it too moving to a DB-backed
registry: blob transfer alone was insufficient.

So the second-Harbor path is genuinely the "safest, slowest" option in the literature —
but only because re-pushing regenerates link objects as a side effect. §4a now does that
directly, proved 760/760 exact against Harbor's own set on staging, which removes the only
real advantage while keeping all 351 robots, 237 quotas and 1,327 untagged artifacts.

### What the Harbor project itself says

**There is no official path.** goharbor #18843 — "Migrate Harbor instance from
Filesystem/Block storage to Object Storage" — was closed **as stale** on 2024-10-29, with
a contributor stating plainly: "It's not stale. There is no progress on this issue from
the dev team yet." Harbor's own docs cover upgrade/data migration (schema and settings),
never storage backends.

The nearest thing to official guidance is in that thread. Contributor **stonezdj**: "You
could do it by replication" — immediately caveated with "the configuration and retention
policy is not replicated. you need to setup manually." Another commenter asked in the same
thread whether replication covers robot accounts; it does not. That is the same wall we
hit independently, from the maintainers' own mouths.

Where practitioners have written the procedure down, it is **our** shape, not replication.
DaoCloud's documented Harbor migration is: put the source in read-only mode, `rclone copy`
the registry storage directory to the object store, and bring the **database across with
it**, with account passwords kept consistent — i.e. blobs and database migrated as a
matched pair inside a read-only window. The Sovereign Cloud Stack writeup does the same,
using read-only mode to keep source and destination in sync.

We are doing that, in a tighter form: same Harbor, same Postgres, no dump/restore at all,
so robots, quotas, memberships and project flags cannot drift. The one deliberate
divergence is that we **generate** `repositories/` from Postgres instead of `rclone`-ing
it. That is forced — the JuiceFS metadata walk did not finish in five minutes for a
handful of the 225 projects — and it drops the ~6k forgotten repository directories for
free. The cost is that it is our code rather than a byte copy, which is what §4a's staging
rehearsal and §7a's sweep exist to cover.

Sign-off needed on D1–D8, then fill §0. Infra runs the helm command. George does not. The hippius-s3 gate is §1.

Success bar after flip: median **≥ 80 MiB/s** on a 1 GiB fresh unique-bytes `hippius-hub` 0.7.0 upload (3 runs).

**First gate 2026-08-25 (bucket `hub-test`, isolated `harbor-staging`, prod Harbor untouched):**

| Check | Result |
|---|---|
| S3 contract (PUT/HEAD/GET/Range/MPU/CopyObject/List/Delete) | **PASS** |
| Raw 64 MiB PUT | 49.3 MiB/s |
| CopyObject 64 MiB (Harbor blob-commit Move, pre-alias) | **4.3 MiB/s** |
| Harbor → hippius-s3 1 GiB hub ×3 | 16.8 / 21.3 / 24.6 → median **21.3 MiB/s** |
| Bar ≥ 80 MiB/s | **FAIL** (also below prod JuiceFS 47) |

**Later the same day (still `harbor-staging`, prod untouched):** hippius-s3 `#445` alias CopyObject, `#448` ingest-node hint, `#451` cipher sizes on that hint, staging `UVICORN_WORKERS=8` / `API_DB_POOL_MAX_SIZE=8`, Harbor `multipartcopythresholdsize=134217728` (kubectl patch of `config.yml` + env; helm rev still 4). Unique 1 GiB ×3 **stock `hippius_hub==0.7.0`**: 10.28s 99.6 / 11.91s 86.0 / 8.81s 116.2 → median **99.6 MiB/s**. Bar **PASS**. Unreleased config-blob overlay on the same Harbor knobs: 9.11s 112.4 / 7.64s 134.1 / 6.90s 148.5 → median 134.1 (optional, not required).

Do **not** helm-upgrade `-n harbor` until `deploy/harbor-s3-prod/overlay-s3.yaml` is the flip file (it now includes `multipartcopythresholdsize=134217728`). Staging Harbor is still up in `harbor-staging` for inspection.

**Read-only review 2026-08-27 — two corrections to the numbers above.**

1. That 99.6 MiB/s was **not** measured against `hippius-s3-prod`. The staging Harbor's
   live `config.yml` reads `regionendpoint: http://gateway.hippius-s3-**staging**…`,
   despite `values-hippius-s3.yaml` and §1 both naming `-prod`. hippius-s3-prod runs
   `api:539eec1` (main, merged staging at 18:35 on 08-25); **#451 landed at 19:33 and is
   not in it**, nor is the reader-TTFB work (#452, #456). `UVICORN_WORKERS=8` and
   `API_DB_POOL_MAX_SIZE=8` exist only in the staging environment ConfigMap — prod uses
   the defaults 4 and 15 — and `HIPPIUS_FS_STORE_SCAN_CONCURRENCY=64` is staging-only.
   Treat the prod number as **unmeasured** until §1 is genuinely re-run (§0.6).
2. That run had **3 registry replicas, 3 core, 3 nginx** plus CPU requests on core and
   nginx. The flip is registry 1 replica, and it deliberately does **not** add resource
   requests to prod's core/nginx (both currently BestEffort at 5 replicas) — introducing
   new scheduling constraints under cover of a storage flip is a separate change. Do not
   quote a staging median as the prod expectation.

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
registry:    deploy/harbor-registry  1 replica  pod on k8s-v3-node1 (39d)
storage:     filesystem  rootdirectory=/storage
PVC:         harbor-registry → pv pvc-b0dac713-…  100Ti RWX  hippius-juicefs
             annotation helm.sh/resource-policy: keep   (present)
S3 gateway:  http://gateway.hippius-s3-prod.svc.cluster.local:8080
```

Two kinds of object have to end up in the bucket. `rootdirectory` is omitted, so keys
start at `docker/`:

```text
# 1. blob bytes — rclone, §4
/storage/docker/registry/v2/blobs/sha256/<aa>/<64-hex>/data
  →  s3://<bucket>/docker/registry/v2/blobs/sha256/<aa>/<64-hex>/data

# 2. per-repository link objects — generated from Postgres, §4a
  →  s3://<bucket>/docker/registry/v2/repositories/<repo>/_layers/sha256/<64-hex>/link
  →  s3://<bucket>/docker/registry/v2/repositories/<repo>/_manifests/revisions/sha256/<64-hex>/link
```

`_manifests/tags/` is genuinely dead on this Harbor — Postgres holds all 26,513 tags and
the directory is empty on every prod repository sampled. `_layers/` and
`_manifests/revisions/` are **not** dead: distribution resolves
`GET /v2/<repo>/blobs/<digest>` through the layer link, and that is the endpoint
`hippius-hub` downloads from (`file_download.py:406,626,656`) and `docker pull` uses.
Verified from the other side too — Harbor's own S3 driver wrote 1,005 `_layers` and 58
`_manifests/revisions` link objects into the staging bucket alongside 386 blobs.

Do not copy `_uploads/` (transient). Do not copy JuiceFS internal chunks. Blob copy size
is the `data` files (~1.9 TB; Postgres says 78,055 blobs / 2,060 GB), **not** JuiceFS
`used_space` 31 TiB. Link objects add ~151,958 keys at 71 bytes.

The disk also carries ~6,000 repository directories the database has already forgotten
(16,234 on disk vs 10,113 live). §4a generates from Postgres, so those orphans are
dropped rather than migrated.

`helm get values harbor -n harbor` (user-supplied) does **not** contain the node1 affinity. That is a live kubectl patch. A helm upgrade without the overlay affinity **drops** it (already happened once).

User-supplied values also contain DB and admin passwords. **Never** `helm get values` into git or Slack. Use `--reuse-values`.

---

## What we are not doing

- New Harbor, new namespace, CAS, `pgdump`/`pgrestore`
- Harbor replication endpoints as the migration (no object-storage target exists, and the
  Harbor-to-Harbor shape loses 351 robots and 1,310 untagged artifacts — see D6). Using
  one as a *rehearsal* against `harbor-staging` is fine.
- JuiceFS `--block-size` / `format --force`
- Client release
- GET 307 / redirect
- Deleting PVC `harbor-registry`
- `helm uninstall harbor`
- Writing into bucket `hippius-juicefs-data`

---

## 0. Fill before any apply

| # | Item | Owner | Notes |
|---|---|---|---|
| 0.1 | Bucket name, **not** `hippius-juicefs-data` | s3 | **Still open.** `hub-test` is the **gate** bucket only (decided 2026-08-27) — it is shared scratch, already holding 26 stray `docker/registry/v2/**` blobs, old `bench` / `phase0-bench` / `harbor-s3-probe` prefixes and an unrelated PNG. Provision a clean bucket (suggested `harbor-registry-cas`) before §4. Replace `REPLACE_ME_BUCKET` in the three YAML files. |
| 0.2 | `hip_` key scoped to that bucket | s3 | Secret `harbor-s3` in ns `harbor`, keys `REGISTRY_STORAGE_S3_ACCESSKEY` / `REGISTRY_STORAGE_S3_SECRETKEY`. The gate key `hub1` verified 2026-08-27: reaches `hub-test`, **403 on `hippius-juicefs-data`** — the credential enforces the stop-the-line rule itself. Scope the real key the same way. |
| 0.3 | The bucket's account can write ~1.9 TB without being gated | George + dubs | **Decided 2026-08-27: a system account carrying a large on-chain balance. No hippius-s3 change.** See the note below. This step is just: confirm it is in place and that a large write actually succeeds, **before** §4 starts a multi-hour copy. |
| 0.4 | Who runs `helm upgrade -n harbor --version 1.19.0` | infra | George does not |
| 0.5 | Chart `harbor/harbor` **1.19.0** still pullable | infra | `helm pull harbor/harbor --version 1.19.0`. Do not upgrade the app. |
| 0.6 | hippius-s3-prod promoted, or the gate re-run and its number accepted | s3 | **Promotion done 2026-08-27 13:31** — prod is now `api:c9ec8b4` (merge of #459), which contains #445, #448, #451, #452 and #456, and `HIPPIUS_FS_STORE_SCAN_CONCURRENCY=64` is in the prod defaults. **Gate run against it at 13:43: upload median 51.5 MiB/s — below the 80 bar.** See §1. Decide: tune and re-gate, or accept 51.5. |
| 0.7 | Harbor **GC schedule paused** | George | `GARBAGE_COLLECTION` cron `0 0 4 * * *`, live since 07-13, ran every day this week. It deletes blobs from storage under the copy and manipulates registry read-only mode under the freeze. |
| 0.8 | Blob census run (`du` + file count) | whoever can apply a Job | **Done 2026-08-27** — `deploy/harbor-s3-prod/blob-census-job.yaml`. **78,552 blob digests on disk** against 78,474 in the database, so only ~78 orphans (0.1%) — the copy set is essentially exactly what Harbor tracks. Logical size **2,060 GB**. Ignore `du`'s 30,720 GiB; see the note below. |
| 0.9 | Bucket's Arion account decided: same as JuiceFS, or separate | s3 | **Answered 2026-08-27: separate.** `hub-test` is owned by `5E4ZQcXV…`; `hippius-juicefs-data` by `5E71kYuD…`. `can_upload` is keyed on the main account, so a 402 on the Harbor bucket will **not** freeze the JuiceFS-backed registry still serving prod. Issue the real bucket under a non-JuiceFS account too, and fund it separately (0.3). |
| 0.10 | **Baseline reachability sweep, before anything changes** | George | §7a's sweep run against today's filesystem-backed prod. It is read-only and works on either backend. Without it, a post-flip failure cannot be told apart from breakage that was already there — the staging rehearsal found 3 artifacts whose manifest blob was already missing from storage while Harbor's DB still referenced them. Save the output. |

### On 0.3 — why funding, and not a code bypass

Both credit gates on the write path reduce to one thing, the account's balance:

1. `gateway/middlewares/account.py:266` — `if not request.state.account.has_credits`, which
   is literally `free_credits > 0` from the chain (`cacher/run_cacher.py:207`).
2. `_check_can_upload` → Arion `POST /can_upload`, a balance-vs-size check.

So a system account with a large balance satisfies both with **no change to hippius-s3**.

Three things verified 2026-08-27 that make this safe, and that are worth not
re-discovering:

- **Miners still get paid.** Gating and usage are separate paths. `can_upload` is a pure
  permission check on the request path and records nothing; the pin that miners are paid
  for is `upload_file_and_get_cid` in `workers/uploader.py:346`, in the async uploader,
  under the same `account_ss58`. Funding changes the gate, not the accounting.
- **Nothing purges an account for being out of credit.** `workers/purger.py` does delete
  and unpin every object in every bucket for an account — but only for one carrying a row
  in `account_suspensions`, and those rows are written solely by the `/admin/*` endpoints
  behind a separate secret. There is no automatic credit-exhaustion → purge path.
- **Do not reach for `HIPPIUS_BYPASS_CREDIT_CHECK`.** `config.py:697` forcibly resets it to
  `False` whenever `ENVIRONMENT != "test"`, so it does nothing in prod — and it is global,
  not per-account: `account.py:196` fabricates `has_credits=True` for every request
  including anonymous ones. If the fence were ever lifted it would disable billing for
  every customer on hippius-s3-prod.

The balance is large, not infinite, so it needs watching. A 402 part-way through §4 stalls
the copy; rclone retries and the Job is re-runnable, so nothing is lost but hours. Check
the balance before §4 and again before §5.

Sizing the funding: **~2 TB of logical bytes**, which is what rclone transfers. See §0.8.

Do **not** size it off `du`, and do not assume the migration slashes the storage bill.
`du -sk` on the blobs tree reports **30,720 GiB**, but that is JuiceFS block accounting,
not bytes: the same 78.5k blobs total **2,060 GB** in Harbor's own database (avg 27 MB,
largest 16 GB), and the JuiceFS bucket holds 8,304,880 live chunk objects for them. Whether
JuiceFS's *billed* footprint is nearer 2 TB or 30 TiB was not measured — the join needed to
total part sizes across 8.3M objects is too heavy to run against prod. So the
storage-cost comparison is **open**, not a saving to bank on.

Helm repo `harbor` → `https://helm.goharbor.io` is already on this machine.

The same bucket + `hip_` key are used first in namespace `harbor-staging` (§1). Do not put the secret in namespace `harbor` until the gate passes.

---

## 1. hippius-s3 gate (required, not MinIO)

Arm C was Harbor → MinIO. This gate is Harbor’s S3 driver against `http://gateway.hippius-s3-prod.svc.cluster.local:8080`. If this fails, stop — do not copy, do not helm prod.

### Gate result — 2026-08-27, first run ever against hippius-s3-prod

Bucket `hub-test`, key `hub1`, endpoint `gateway.hippius-s3-prod`, prod on the promoted
`api:c9ec8b4`. Isolated `harbor-staging`; prod Harbor untouched (still helm rev 4,
filesystem).

| Check | Result |
|---|---|
| S3 contract (PUT/HEAD/GET/Range/MPU/CopyObject/List/Delete) | **PASS** |
| Raw 64 MiB PUT | **94.3 MiB/s** (staging's first gate: 49.3) |
| CopyObject 64 MiB — the Harbor blob-commit Move | **344.2 MiB/s** (first gate: 4.3) |
| Unique 1 GiB `hippius_hub` 0.7.0 upload ×3 | **median 51.5 MiB/s** (19.89 s) |
| Bar ≥ 80 MiB/s | **FAIL** |
| Unique 1 GiB download ×3 | **median 200.4 MiB/s** (5.11 s) |

Two things this settles. The **CopyObject alias path works on prod** — 344 MiB/s against
4.3 on the pre-#445 gate, so the original blocker is gone. And the **read path is not a
problem**: 200 MiB/s, ~4× the write rate, the first pull number anyone has measured. The
worry that `disableredirect` would bottleneck pulls through the registry does not show up
here.

Upload is still short of the bar. The progression is 21.3 (no alias) → **51.5** (prod,
promoted) → 99.6 (staging, 2026-08-25). Prod and staging now run the *same code*, so the
remaining difference is configuration and load:

- `UVICORN_WORKERS` 4 on prod vs 8 on staging; `API_DB_POOL_MAX_SIZE` 15 vs 8.
- 5 api-local pods on prod vs 2 on staging — more cross-node peer fetches, which is the
  known source of the 20 s blob-commit tails.
- prod carries real traffic; staging was idle.

Treat "workers/pool explains the 2×" as the leading hypothesis, **not** a finding. The
cheap test is a ConfigMap change plus a re-gate.

Both numbers are **optimistic for prod**: this ran registry ×3, core ×3, nginx ×3 with CPU
requests on registry. The flip is registry ×1 and adds no core/nginx requests.

```bash
export KUBECONFIG=~/Hippius-Storage/Configs/k8s/hippius.yaml
export HARBOR_S3_BUCKET=<0.1>
export HARBOR_S3_ACCESS_KEY=hip_...
export HARBOR_S3_SECRET_KEY=...
./deploy/harbor-staging/install-hippius-s3.sh
```

That script, in `harbor-staging` only:

1. Contract Job (`deploy/harbor-s3-prod/hippius_s3_contract.py`): path-style SigV4, tiny PUT, 64 MiB PUT, Range GET, MPU, CopyObject (Harbor blob-commit Move), List, Delete. Writes only under `harbor-s3-probe/<run-id>/` and deletes it. 402 credits = fail.
2. Helm Harbor 1.19.0 with `values-hippius-s3.yaml` (same extraEnvVars as the prod overlay).
3. 1 GiB `hippius-hub` 0.7.0 × 3 (`arm-c-job.yaml`). Median **≥ 80 MiB/s**.

Tear down when done: `./deploy/harbor-staging/uninstall.sh` (refuses ns `harbor`).

Do not treat the MinIO 93 MiB/s number as this gate.

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
- `deploy/harbor-s3-prod/link-objects-job.yaml`

Also confirm the two live values Helm does not know about, because the overlay now
re-supplies them and a drift here silently changes what the flip renders:

```bash
kubectl -n harbor get deploy harbor-core  -o jsonpath='{.spec.replicas}{"\n"}'   # expect 5
kubectl -n harbor get deploy harbor-nginx -o jsonpath='{.spec.replicas}{"\n"}'   # expect 5
kubectl -n harbor get deploy harbor-registry \
  -o jsonpath='{.spec.template.spec.affinity.nodeAffinity}{"\n"}'                # expect node1
```

If any differs, update `overlay-s3.yaml` and `overlay-filesystem-rollback.yaml` before
the dry-run. Neither core nor nginx is under an HPA (the only one in the namespace is
KEDA on `harbor-warmer-worker`, which is not Helm-managed).

Pause GC now (0.7) and record the previous schedule so §9 can restore it.

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
| `redirect.disable: true` | NodePort / postgres / `externalURL` rewritten |
| env `REGISTRY_STORAGE_S3_FORCEPATHSTYLE=true` | affinity dropped |
| env `REGISTRY_STORAGE_S3_SECURE=false` | chart version ≠ 1.19.0 |
| `core.replicas: 5` and `nginx.replicas: 5` | affinity dropped |

**The rendered manifest will not contain PVC `harbor-registry`, and that is correct.**
Chart 1.19.0 emits that PVC only when `type == filesystem`. What protects it is the
`helm.sh/resource-policy: keep` annotation already on the live claim, which Helm honors
on upgrade, not just uninstall — verified present 2026-08-27. Do not abort because the
dry-run output lacks the PVC; check instead that it is still `Bound` after §6.

If dry-run rewrites NodePort 30002, touches postgres, changes `externalURL`, bumps the
chart, or drops the affinity or the replica counts: **stop**.

---

## 4. Online copy (Harbor stays serving)

Job mounts PVC `harbor-registry` read-only (RWX). Registry pod is not restarted.

```bash
kubectl -n harbor apply -f deploy/harbor-s3-prod/copy-blobs-job.yaml
kubectl -n harbor logs -f job/harbor-blob-copy
```

Sync:

```text
/storage/docker/registry/v2/blobs/**/data
  →  s3://<bucket>/docker/registry/v2/blobs/sha256/<aa>/<digest>/data
```

`--size-only`: skip objects whose size already matches. Re-run the Job (delete + apply) until a pass copies ~0 bytes.

Retry on 503 / SlowDown is in rclone flags. Do not point rclone at `hippius-juicefs-data`.

Sample check (on the Job pod or a one-off): SHA-256 of local `…/sha256/<aa>/<digest>/data` equals `<digest>`; S3 `HEAD` size equals file size. Do this for ≥20 random blobs including one large pack.

Expected wall clock: hours, not minutes (~1.9 TB). That is the long pole. **Do not freeze yet.**

Size check before copy (**0.8 — required, not optional**):

```bash
kubectl -n harbor run harbor-blob-du --rm -it --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"du","image":"busybox:1.37","command":["sh","-c","du -sh /storage/docker/registry/v2/blobs; find /storage/docker/registry/v2/blobs -type f -name data | wc -l"],"volumeMounts":[{"name":"r","mountPath":"/storage","readOnly":true}]}],"volumes":[{"name":"r","persistentVolumeClaim":{"claimName":"harbor-registry"}}]}}' \
  --image=busybox:1.37
```

---

## 4a. Link objects (required — the copy alone leaves every artifact 404)

`copy-blobs-job.yaml` moves bytes. Distribution will not serve those bytes for a
repository without its link objects, and Harbor has no tool to rebuild them.

Generated from Harbor's Postgres rather than walked off JuiceFS: the walk did not finish
in five minutes for a handful of the 225 projects, and generating from the database
produces exactly the live set instead of dragging ~6k forgotten repository directories
across.

Derivation, verified against the live filesystem 2026-08-27:

```text
_layers    every distinct (repository_name, digest_blob) in artifact ⋈ artifact_blob,
           EXCLUDING any digest_blob that is also an artifact digest in that repository
           — Harbor records the manifest among a repo's blobs, but distribution keeps
           manifests under _manifests/revisions, never _layers
revisions  every (repository_name, artifact.digest)
body       "sha256:<hex>"   71 bytes, no trailing newline
```

Spot-check behind that rule: `cascade/ckpt-r17988…-u56` lists 6 blob digests in the DB;
disk holds 5 `_layers` links and 1 revision link, and the digest missing from `_layers`
is exactly the artifact's own.

```bash
kubectl -n harbor create configmap harbor-link-objects \
  --from-file=harbor_link_objects.py=deploy/harbor-s3-prod/harbor_link_objects.py \
  --dry-run=client -o yaml | kubectl apply -f -

# MODE=plan first: counts and samples keys, writes nothing.
kubectl -n harbor apply -f deploy/harbor-s3-prod/link-objects-job.yaml
kubectl -n harbor logs -f job/harbor-link-objects
# expect ~151,958 total link objects. If it reports 0, stop — the query is wrong.

# then MODE=apply, then MODE=verify
kubectl -n harbor delete job harbor-link-objects
# edit MODE in link-objects-job.yaml, re-apply
```

PUT is idempotent, so a partial failure just means delete the Job and re-apply. Run this
**after** §4's blob copy has caught up and again in §5, since anything pushed between the
two runs needs its links too.

**Measured on harbor-staging 2026-08-27:** 817 link objects in 12.5 s = **66/s at 16
workers**. At that rate prod's 151,960 objects is ~38 min; the prod Job runs 32 workers,
so expect somewhere in 20–40 min. This is not the long pole — §4's ~1.9 TB is. Numbers
are from hippius-s3-staging (2 api-local pods); prod has 5 but also carries real load.

Rehearsal, if you want independent confirmation of the shapes: point a Harbor-to-Harbor
replication rule from prod at the S3-backed `harbor-staging` and diff the link keys
Harbor writes itself against what `MODE=plan` lists. That touches no prod storage.

---

## 5. Freeze (minutes)

Only when §4 catch-up is idle.

```bash
ADMIN=$(kubectl -n harbor get secret harbor-core \
  -o jsonpath='{.data.HARBOR_ADMIN_PASSWORD}' | base64 -d)

# freeze pushes (pulls stay up until the registry restart)
curl -sS -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":true}'

curl -sS -u "admin:${ADMIN}" \
  https://registry.hippius.com/api/v2.0/configurations | grep read_only
```

GC must already be paused (§0.7). **Confirmed against Harbor's docs 2026-08-27:** "When GC
runs, Harbor goes into read-only mode and all modifications to the registry are
prohibited." It restores the value it captured on entry, so a run that starts before this
`read_only: true` lands would lift the freeze when it finishes, and anything pushed after
the final catch-up would exist only on JuiceFS. Confirm no GC is running before continuing.

One thing the same research clears up, in our favour: Harbor 2.x's GC is **DB-driven**,
marking from the `blob` table rather than walking storage the way upstream distribution's
`registry garbage-collect` does. So an incomplete §4a cannot cause GC to delete blobs it
thinks are unreferenced — the failure mode is 404s on pull, not deletion. The flip side is
that storage-only orphans are invisible to GC forever (goharbor #23199), so the ~78 orphan
blobs §0.8 found will simply sit in the bucket.

1. Announce: no pushes.
2. Wait in-flight uploads to finish or fail (they retry after).
3. Delete + re-apply `copy-blobs-job.yaml` once more (should be seconds–minutes).
4. Delete + re-apply `link-objects-job.yaml` with `MODE=apply`, then `MODE=verify`.
5. Scale jobservice to 0 — its PVC is RWO on ceph-block, and if the helm rollout
   schedules the new pod on another node the Multi-Attach wedges the release in
   `pending-upgrade` (this is what happened on harbor-staging):

   ```bash
   kubectl -n harbor scale deploy/harbor-jobservice --replicas=0
   kubectl -n harbor wait --for=delete pod -l component=jobservice --timeout=2m
   ```

6. Go to §6 immediately.

---

## 6. Flip (the only prod Helm upgrade)

```bash
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values \
  -f deploy/harbor-s3-prod/overlay-s3.yaml
```

Pushes **and** pulls 502 while `harbor-registry` restarts (minutes). Postgres is not restarted.

```bash
kubectl -n harbor rollout status deploy/harbor-registry --timeout=5m
kubectl -n harbor get cm harbor-registry -o jsonpath='{.data.config\.yml}'
# must show storage.s3, not storage.filesystem

kubectl -n harbor get deploy harbor-registry -o jsonpath='{.spec.template.spec.affinity}{"\n"}'
# must still prefer k8s-v3-node1

kubectl -n harbor get pvc harbor-registry
# must still be Bound

kubectl -n harbor get deploy harbor-registry \
  -o jsonpath='{.spec.template.spec.containers[0].env}'
# must include REGISTRY_STORAGE_S3_FORCEPATHSTYLE=true
# and REGISTRY_STORAGE_S3_SECURE=false

kubectl -n harbor get deploy harbor-core harbor-nginx \
  -o jsonpath='{range .items[*]}{.metadata.name}={.spec.replicas}{"\n"}{end}'
# must be 5 and 5 — if helm reverted them to 1, the overlay lost its replica block

kubectl -n harbor scale deploy/harbor-jobservice --replicas=1
```

Bring jobservice back up (scaled to 0 in §5.5) once the rollout is complete.

---

## 7. Prove (before anyone calls it done)

**Order matters.** The unique 1 GiB upload cannot run while Harbor is `read_only`, so the
old instruction to "keep read_only until (1) and (2) pass" was not executable. Do the
read-side proof under the freeze — that is the half that rolls back cheaply — then
unfreeze, then measure writes.

### 7a. Under the freeze, before unfreezing

**First flush the registry's blob-descriptor cache**, or the sweep can pass on
pre-flip data. `storage.cache.layerinfo: redis` is repository-scoped — keys are
`repository::<repo>::blobs::<digest>` — and redis is a separate StatefulSet that
survives the registry restart. A cached descriptor answers without reading the link
object or S3 at all (3,626 entries were live on 2026-08-27).

```bash
kubectl -n harbor exec harbor-redis-0 -- redis-cli -n 2 DBSIZE
kubectl -n harbor exec harbor-redis-0 -- redis-cli -n 2 FLUSHDB
# it is a cache; the registry repopulates it.
# db 2 ONLY. Verified 2026-08-27: db 0 = core (_REDIS_URL_CORE, 20,837 keys — sessions),
# db 1 = jobservice queues (1,220), db 2 = _REDIS_URL_REG (3,628), db 3 = warmer.
# Flushing 0 or 1 would log everyone out / drop queued jobs.
```

Full reachability sweep — every artifact, both link classes. The sweep uses ranged
GETs rather than HEADs for the same reason, so it reads real bytes (one per layer):

```bash
kubectl -n harbor create configmap harbor-reachability-sweep \
  --from-file=harbor_reachability_sweep.py=deploy/harbor-s3-prod/harbor_reachability_sweep.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n harbor apply -f deploy/harbor-s3-prod/reachability-sweep-job.yaml
kubectl -n harbor logs -f job/harbor-reachability-sweep
# expect: swept ~27,775 artifacts / PASS every artifact resolves
```

Compare against the §0.10 baseline. The pass condition is **no failures that were not
already failing before the flip** — not an unconditional PASS. The sweep detects any
Harbor-DB-to-storage inconsistency, including artifacts that were already unservable, so
a bare count is not enough to judge the migration by.

Rehearsed end to end on harbor-staging 2026-08-27 against Harbor's own data: with the
link objects deleted the sweep failed 32 of 43 artifacts with
`manifest GET 404 (missing _manifests/revisions link)`; after §4a regenerated them it
failed only the 3 whose manifest blob was independently missing from the bucket. For
every healthy artifact the generated key set reproduced Harbor's own, exactly — 760 of
760, nothing missing, nothing spurious.

Then, still frozen:

1. Download of a **copied** public model that was **not** re-uploaded (proves keys).
2. `docker pull` of a known tag.
3. Anonymous pull of a public project.
4. Existing `robot$…` login (no rotate).
5. `hippius-hub registry me` / provision still talk to the same Harbor API.
6. SHA-256 of ≥20 sampled blobs, including one large pack, matches the digest in the key.

Any failure here → roll back (§8). Because nothing has been written to S3 since the
freeze, rollback costs nothing.

### 7b. Unfreeze, then measure writes

```bash
curl -sS -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":false}'
```

7. `hippius-hub` 0.7.0, 1 GiB **fresh** bytes, 3 runs. Median **≥ 80 MiB/s**.
8. Pull throughput on a 1 GiB artifact, 3 runs — record the median.

(8) has no floor yet because nothing has ever measured it; the gate and the bench are
upload-only. Record it so the next change has a baseline. It is the number most affected
by the flip: `disableredirect: true` proxies every pulled byte through a single registry
replica, and hippius-s3-prod is missing the reader-TTFB work.

If (7) fails but pulls are healthy: stay on S3 and treat throughput as a follow-up. Do
not retune JuiceFS. Compare against prod's own pre-flip numbers, not a staging median.

---

## 8. Rollback (minutes)

JuiceFS PVC is still Bound and still has pre-flip bytes.

```bash
# freeze again if you already unfroze
curl -sS -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":true}'

helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values \
  -f deploy/harbor-s3-prod/overlay-filesystem-rollback.yaml

kubectl -n harbor rollout status deploy/harbor-registry --timeout=5m
# config.yml must show filesystem /storage again
# PVC still Bound; affinity still node1

curl -sS -u "admin:${ADMIN}" \
  -H 'Content-Type: application/json' \
  -X PUT https://registry.hippius.com/api/v2.0/configurations \
  -d '{"read_only":false}'
```

Scale jobservice to 0 before this helm upgrade too, and back to 1 after — same RWO
Multi-Attach hazard as §5.5.

Blobs **pushed during the S3 window** are not on JuiceFS. Rollback after a long S3 write window without a reverse copy **drops those blobs**. That is why §7a runs before the unfreeze.

Do not delete the PVC. Do not `juicefs format --force`.

---

## 9. After the flip

Same day:

- Re-enable the GC schedule paused in §0.7, and watch the first run. It now sweeps S3.
- Re-run §7a's sweep once after ~24h of live pushes: it costs nothing and confirms the
  link objects are being written by Harbor itself, not just by §4a.

After ~7 days of S3 as source of truth you may stop relying on the JuiceFS PVC. Separate
change. Until then ~1.9 TiB is stored twice, plus the 9.7M JuiceFS chunk objects.

---

## Owners

| Step | Who |
|---|---|
| 0.1–0.3 bucket + key + credits | s3 |
| 1 hippius-s3 gate (`install-hippius-s3.sh`) | George |
| 0.4–0.5, 3, 6, 8 helm | infra |
| 4 copy Job | whoever can `kubectl apply` a Job that mounts `harbor-registry` |
| 4a link objects | whoever can `kubectl apply` a Job in ns `harbor` |
| 5 / 6 freeze + unfreeze | George + infra |
| 7a reachability sweep (under freeze) | George |
| 7b prod prove | George (`benchmark.yml` / `test/e2e-client`) |
| 9 re-enable GC | George |

---

## Stop-the-line

- Dry-run changes NodePort, postgres, `externalURL`, or chart version
- Dry-run renders `core.replicas` or `nginx.replicas` as 1
- PVC `harbor-registry` is not `Bound` after §6 (its absence from the rendered
  manifest is expected — see §3)
- §4a `MODE=plan` reports 0 link objects, or a count nowhere near ~152k
- §7a reports any unreachable artifact
- Unfreezing before §7a passes
- GC schedule still active during copy or freeze
- Enabling GC's **"Delete Untagged Artifacts"**. Prod's schedule has
  `delete_untagged: false` and must keep it: the flag *deletes* untagged artifacts rather
  than merely collecting already-deleted ones (goharbor #16326), and prod holds **1,327**
  of them — recent, and reachable by digest through `hippius-hub`.
- Converting any project to a **proxy cache** to "mirror" content: they cannot be pushed
  to, and carry a default 7-day retention that evicts what they cache
- Copy destination is `hippius-juicefs-data`
- `helm upgrade` without `--reuse-values` or without `--version 1.19.0`
- `helm upgrade` without the node1 affinity overlay
- Dump/restore of `harbor-postgres`
- `helm uninstall harbor`
- Any helm command in a namespace other than `harbor`
- Enabling redirect / 307 on this flip
- Skipping §1 (MinIO arm C is not this gate)
- Bucket or endpoint containing `juicefs` or `minio`
