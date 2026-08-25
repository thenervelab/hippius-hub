#!/usr/bin/env bash
# Isolated Harbor + hippius-s3 contract + 1 GiB hub bench.
# Refuses namespace harbor (prod). Refuses bucket hippius-juicefs-data.
# Requires: HARBOR_S3_BUCKET HARBOR_S3_ACCESS_KEY HARBOR_S3_SECRET_KEY
set -euo pipefail

NS=harbor-staging
RELEASE=harbor-staging
CHART=harbor/harbor
CHART_VERSION=1.19.0
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
# After hippius-s3 staging is promoted to prod, this default is the gate
# that must pass before helm -n harbor. For a staging-gateway re-run:
#   HARBOR_S3_ENDPOINT=http://gateway.hippius-s3-staging.svc.cluster.local:8080
ENDPOINT="${HARBOR_S3_ENDPOINT:-http://gateway.hippius-s3-prod.svc.cluster.local:8080}"

if [[ "${1:-}" == "--yes-prod" ]]; then
  echo "this script has no prod mode" >&2
  exit 2
fi
if [[ "${HELM_NAMESPACE:-}" == "harbor" ]] || [[ "${NS}" == "harbor" ]]; then
  echo "refusing to operate on namespace harbor (production)" >&2
  exit 2
fi

BUCKET="${HARBOR_S3_BUCKET:?set HARBOR_S3_BUCKET}"
ACCESS="${HARBOR_S3_ACCESS_KEY:?set HARBOR_S3_ACCESS_KEY}"
SECRET="${HARBOR_S3_SECRET_KEY:?set HARBOR_S3_SECRET_KEY}"

if [[ "${BUCKET}" == "hippius-juicefs-data" ]] || [[ "${BUCKET}" == *juicefs* ]]; then
  echo "refusing bucket ${BUCKET}" >&2
  exit 2
fi
if [[ "${ENDPOINT}" == *minio* ]]; then
  echo "refusing MinIO endpoint ${ENDPOINT}" >&2
  exit 2
fi

export KUBECONFIG="${KUBECONFIG:-${HOME}/Hippius-Storage/Configs/k8s/hippius.yaml}"

kretry() {
  local n=0
  local max=8
  until "$@"; do
    n=$((n + 1))
    if (( n >= max )); then
      echo "failed after ${max} tries: $*" >&2
      return 1
    fi
    echo "retry ${n}/${max}: $*" >&2
    sleep $((n * 3))
  done
}

if helm list -n harbor --filter '^harbor$' 2>/dev/null | grep -q deployed; then
  echo "prod helm release 'harbor' in namespace harbor is present; will not upgrade it"
fi
if helm list -n harbor -q 2>/dev/null | grep -qx "${RELEASE}"; then
  echo "refusing: a release named ${RELEASE} already exists in namespace harbor" >&2
  exit 2
fi

kretry kubectl apply -f "${HERE}/namespace.yaml"

if kubectl -n "${NS}" get secret harbor-staging-s3 >/dev/null 2>&1; then
  echo "replacing harbor-staging-s3 secret"
  kubectl -n "${NS}" delete secret harbor-staging-s3
fi
kretry kubectl -n "${NS}" create secret generic harbor-staging-s3 \
  --from-literal=REGISTRY_STORAGE_S3_ACCESSKEY="${ACCESS}" \
  --from-literal=REGISTRY_STORAGE_S3_SECRETKEY="${SECRET}" \
  --from-literal=HARBOR_S3_BUCKET="${BUCKET}" \
  --from-literal=HARBOR_S3_ENDPOINT="${ENDPOINT}"

if ! kubectl -n "${NS}" get secret harbor-staging-admin >/dev/null 2>&1; then
  admin="$(openssl rand -base64 18 | tr -d '/+=' | head -c 24)"
  kretry kubectl -n "${NS}" create secret generic harbor-staging-admin \
    --from-literal=HARBOR_ADMIN_PASSWORD="${admin}"
fi

kretry kubectl -n "${NS}" create configmap hippius-s3-contract \
  --from-file=hippius_s3_contract.py="${ROOT}/deploy/harbor-s3-prod/hippius_s3_contract.py" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NS}" delete job hippius-s3-contract --ignore-not-found
kretry kubectl apply -f "${ROOT}/deploy/harbor-s3-prod/hippius-s3-contract-job.yaml"
echo "waiting for hippius-s3 contract (Harbor driver ops, not MinIO)"
if ! kubectl -n "${NS}" wait --for=condition=complete job/hippius-s3-contract --timeout=20m; then
  kubectl -n "${NS}" logs job/hippius-s3-contract || true
  echo "hippius-s3 contract failed — not installing Harbor" >&2
  exit 1
fi
kubectl -n "${NS}" logs job/hippius-s3-contract

ADMIN_PASS="$(kretry kubectl -n "${NS}" get secret harbor-staging-admin \
  -o jsonpath='{.data.HARBOR_ADMIN_PASSWORD}' | base64 -d)"

helm repo add harbor https://helm.goharbor.io >/dev/null 2>&1 || true
helm repo update harbor >/dev/null

kretry helm upgrade --install "${RELEASE}" "${CHART}" \
  --version "${CHART_VERSION}" \
  --namespace "${NS}" \
  --create-namespace \
  -f "${HERE}/values-hippius-s3.yaml" \
  --set harborAdminPassword="${ADMIN_PASS}" \
  --set-string persistence.imageChartStorage.s3.bucket="${BUCKET}" \
  --set-string persistence.imageChartStorage.s3.regionendpoint="${ENDPOINT}" \
  --wait \
  --timeout 10m

echo "installed ${RELEASE} in namespace ${NS} on hippius-s3 bucket ${BUCKET}"

kretry kubectl -n "${NS}" create configmap arm-c-bench \
  --from-file=arm_c_bench.py="${HERE}/arm_c_bench.py" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NS}" delete job arm-c-bench --ignore-not-found
kretry kubectl apply -f "${HERE}/arm-c-job.yaml"
echo "waiting for 1 GiB hub bench against Harbor → hippius-s3"
if ! kubectl -n "${NS}" wait --for=condition=complete job/arm-c-bench --timeout=40m; then
  kubectl -n "${NS}" logs job/arm-c-bench || true
  echo "arm C on hippius-s3 failed" >&2
  exit 1
fi
kubectl -n "${NS}" logs job/arm-c-bench

echo
echo "compare: prod JuiceFS 47 MiB/s  |  MinIO arm C 93 MiB/s  |  this run above"
echo "success bar: median >= 80 MiB/s"
echo "tear down: ${HERE}/uninstall.sh"
echo "do not helm upgrade -n harbor"
