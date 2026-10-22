# Prod Harbor S3 flip artifacts

Do not apply until `docs/plans/2026-08-25-harbor-s3-prod-runbook.md` §0 is filled and
the §1 gate against `gateway.hippius-s3-prod` has passed.

| File | Runbook step | Use |
|---|---|---|
| `hippius_s3_contract.py`, `hippius-s3-contract-job.yaml` | §1 | Harbor-shaped S3 ops against the prod gateway. CopyObject 64 MiB below 50 MiB/s is FAIL (streaming, not alias). |
| `blob-census-job.yaml` | §0.8 | Read-only count and size of the blob tree on the JuiceFS PVC. |
| `secret-harbor-s3.yaml` | §2 | Bucket-scoped `hip_` key. Fill, never commit values. |
| `overlay-s3.yaml` | §3, §6 | The only prod helm overlay. `--reuse-values --version 1.19.0`. Carries the 5 GiB copy threshold, path-style and insecure env, node1 affinity, core/nginx replica counts. |
| `copy-blobs-job.yaml` | §4 | Online rclone copy of `blobs/**/data`, `--size-only`, re-runnable for catch-up. |
| `harbor_link_objects.py`, `link-objects-job.yaml` | §4a | Generate the `_layers` and `_manifests/revisions` link objects from Harbor's Postgres. `plan`, then `apply`, then `verify`. |
| `harbor_reachability_sweep.py`, `reachability-sweep-job.yaml` | §0.10, §7a | Ranged GET of every artifact's manifest and one layer through Harbor. Baseline before anything changes; again under the freeze. |
| `overlay-filesystem-rollback.yaml` | §8 | Back to JuiceFS. Restores the same replica counts and affinity as the flip. |
| `redis-ha.yaml` | Redis HA cutover | Operator-managed Redis for Harbor: 1 master + 2 replicas and 3 Sentinels, each pinned to a different node. |
| `overlay-redis-ha.yaml` | Redis HA cutover | Points Harbor at the Sentinels. Removes the chart's single-pod Redis. |
| `overlay-ha.yaml` | HA overlay | Job logs to Postgres instead of a ReadWriteOnce volume; 2 registry pods spread across nodes. |
| `x509-exporter-rbac.yaml` | Token certificate | Role + RoleBinding letting hippius-otel's x509-certificate-exporter read Secrets in harbor, for the token-cert expiry alerts. |

Never `helm upgrade` / `helm uninstall` in ns `harbor` except the flip in that runbook,
the Redis HA cutover and the HA overlay below.
Never bucket `hippius-juicefs-data`. Never a copy threshold above 5 GiB.

The overlays compose with `--reuse-values`: a later S3 flip or rollback keeps Redis
external, and the Redis cutover keeps the S3 storage settings.

**`--reuse-values` reuses the last *successful* revision, not the latest one.** If
`helm history harbor -n harbor` ends in `failed`, it silently drops everything that
revision applied. On 2026-10-22 revision 6 (the Redis cutover) was marked failed by a
`--wait` timeout while fully applied, and a `--reuse-values` dry run on top of it
re-added the chart's `harbor-redis` and pointed Harbor back at it. Always read a
`--dry-run=server` first. If the last revision is failed, pass its values explicitly:
`--reset-values -f <(helm get values harbor -n harbor --revision N) -f <overlay>`.
That output holds the DB and admin passwords: keep it on disk only, never in git.

## Redis HA cutover (done 2026-10-22)

Why: on 2026-10-22 node4 went NotReady with `harbor-redis-0` on it. The pod sat
`Terminating`, every `harbor-core` crash-looped on `failed to initialize cache`, and
the registry served 502 until the pod was force-deleted and its VolumeAttachment
removed by hand. `redis-ha.yaml` has the details.

Applied as written below. What differed from the plan, for whoever does this again:

- `redis-ha.yaml` first ran without `podSecurityContext`. Both the Redis pods
  (`appendonlydir: Permission denied`) and the Sentinels (`sentinel.conf is not
  writable`) crash-looped. `kubectl apply --dry-run=server` validates the schema only
  and never starts a pod. Fixed in #123 and #124.
- Step 3 deadlocked for 10 minutes on jobservice: its ReadWriteOnce log volume could
  not attach to the new pod while the old pod held it. Unblocked by switching that
  Deployment to `Recreate` by hand. `overlay-ha.yaml` removes the volume so it cannot
  recur. Helm marked the revision failed on the `--wait` timeout although every
  resource was applied, which is the trap described above.
- Key copy: counts matched in every DB (db0 12,062, db1 538, db2 2,543, db3 2).
- Failover drill, 13:22Z: Sentinel promoted a replica in 8s. 3 of 75 `/v2/` probes
  timed out and every `/api/v2.0/ping` returned 200. No Harbor pod restarted. The
  operator moved `harbor-redis-ha-master` to the new master; harbor-warmer, which
  uses that Service, saw errors for about 27s and recovered without a restart.
- `data-harbor-redis-0` was deleted after the drill. There is no rollback to the
  chart's Redis with data: `--set redis.type=internal` would start it empty.

```sh
# 1. Create the new Redis. Wait for a master and three pods on three nodes.
kubectl apply -n harbor -f redis-ha.yaml
kubectl -n harbor get redisreplication harbor-redis-ha -o jsonpath='{.status.masterNode}'
kubectl -n harbor get pods -l 'app in (harbor-redis-ha,harbor-redis-ha-sentinel)' -o wide
kubectl -n harbor exec harbor-redis-ha-sentinel-0 -- \
  redis-cli -p 26379 SENTINEL get-master-addr-by-name myMaster

# 2. Copy every key from the old Redis (db0 core, db1 jobservice, db2 registry,
#    db3 harbor-warmer, db5 trivy). COPY leaves the source intact for rollback.
kubectl -n harbor exec harbor-redis-0 -- sh -c 'for db in 0 1 2 3 5; do
  redis-cli -n $db --scan | xargs -r -n 200 \
    redis-cli -n $db MIGRATE harbor-redis-ha-master 6379 "" $db 5000 COPY REPLACE KEYS
  echo "db$db old=$(redis-cli -n $db dbsize) new=$(redis-cli -h harbor-redis-ha-master -n $db dbsize)"
done'

# 3. Switch Harbor.
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor --reuse-values \
  -f overlay-redis-ha.yaml --wait --timeout 10m

# 4. Merge thenervelab/harbor-warmer#1: its CI applies REDIS_URL
#    redis://harbor-redis-ha-master:6379/3.
```

5. Verify: `/v2/` is 401, `/api/v2.0/ping` is 200, and the core, jobservice, registry
   and trivy logs show no Redis errors.
6. Failover drill: delete the pod named by `.status.masterNode`, then check that the
   endpoints of `harbor-redis-ha-master` list the new master.
7. Delete PVC `data-harbor-redis-0`.

## HA overlay

`overlay-ha.yaml` sends job logs to Postgres, runs two registry pods, and signs tokens
with the fixed certificate in `harbor-token-service` (below). Each change rolls out as a
rolling update.

```sh
helm history harbor -n harbor --max 1    # last revision failed? see the trap above
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor --reuse-values \
  -f overlay-ha.yaml --dry-run=server     # expect: no harbor-redis, registry replicas 2
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor --reuse-values \
  -f overlay-ha.yaml --wait --timeout 10m
```

Verify: two registry pods on different nodes, `/v2/` is 401, and a job log opens in
the UI. The chart stops managing the old `harbor-jobservice` PVC but keeps it
(`resourcePolicy: keep`). Once a new job's log reads back from the database, delete it:
`kubectl -n harbor delete pvc harbor-jobservice`. It only held file job logs, which
the sweeper deletes after 14 days anyway.

## Token certificate

Core signs registry tokens with the key in Secret `harbor-token-service`
(`core.secretName` in `overlay-ha.yaml`). Without it the chart generates a new key on
every `helm upgrade`, and during the rollout old and new core pods reject each other's
tokens: pushes fail with `401 unauthorized ... action: push`, as they did for about a
minute during revision 7 on 2026-10-22.

The Secret holds a private key, so it is created by hand and never committed. It was
created on 2026-10-22 from the certificate revision 7 had generated:

```sh
kubectl -n harbor get secret harbor-core -o json | jq '{apiVersion:"v1", kind:"Secret",
  type:"kubernetes.io/tls", metadata:{name:"harbor-token-service", namespace:"harbor"},
  data:{"tls.crt":.data["tls.crt"], "tls.key":.data["tls.key"]}}' | kubectl apply -f -
```

**It expires 2027-10-22 13:57:06Z** (annotation `hippius.io/not-after`). Rotating it
invalidates every outstanding token once, which clients get past by logging in again:
generate a new key and certificate, replace the Secret's two keys, then
`kubectl -n harbor rollout restart deploy/harbor-core`. Core reads the key through a
`subPath` mount, which does not follow Secret updates, so the restart is what applies it.

Its expiry is alerted by `HarborTokenCertExpiringSoon` (30 days), `HarborTokenCertExpired`
and `HarborTokenCertMetricMissing` in thenervelab/hippius-otel
`alerting/rules/hippius-harbor.yaml`. The exporter reads the Secret through
`x509-exporter-rbac.yaml` in this directory, applied by hand.
