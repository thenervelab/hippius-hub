#!/usr/bin/env bash
# Install Harbor+MinIO into namespace harbor-staging only.
# Refuses to touch namespace harbor (prod).
set -euo pipefail

NS=harbor-staging
RELEASE=harbor-staging
CHART=harbor/harbor
CHART_VERSION=1.19.0
HERE="$(cd "$(dirname "$0")" && pwd)"

if [[ "${1:-}" == "--yes-prod" ]]; then
  echo "this script has no prod mode" >&2
  exit 2
fi

if [[ "${HELM_NAMESPACE:-}" == "harbor" ]] || [[ "${NS}" == "harbor" ]]; then
  echo "refusing to operate on namespace harbor (production)" >&2
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

existing="$(helm list -n harbor -q 2>/dev/null || true)"
if echo "${existing}" | grep -qx "${RELEASE}"; then
  echo "refusing: a release named ${RELEASE} already exists in namespace harbor" >&2
  exit 2
fi

kretry kubectl apply -f "${HERE}/namespace.yaml"

if kubectl -n "${NS}" get secret harbor-staging-s3 >/dev/null 2>&1; then
  echo "reusing existing harbor-staging-s3 secret"
else
  user="harborstaging"
  pass="$(openssl rand -base64 24 | tr -d '/+=' | head -c 32)"
  admin="$(openssl rand -base64 18 | tr -d '/+=' | head -c 24)"
  kretry kubectl -n "${NS}" create secret generic harbor-staging-s3 \
    --from-literal=MINIO_ROOT_USER="${user}" \
    --from-literal=MINIO_ROOT_PASSWORD="${pass}" \
    --from-literal=REGISTRY_STORAGE_S3_ACCESSKEY="${user}" \
    --from-literal=REGISTRY_STORAGE_S3_SECRETKEY="${pass}"
  kretry kubectl -n "${NS}" create secret generic harbor-staging-admin \
    --from-literal=HARBOR_ADMIN_PASSWORD="${admin}"
  echo "created secrets harbor-staging-s3 and harbor-staging-admin in ${NS}"
fi

if ! kubectl -n "${NS}" get secret harbor-staging-admin >/dev/null 2>&1; then
  admin="$(openssl rand -base64 18 | tr -d '/+=' | head -c 24)"
  kretry kubectl -n "${NS}" create secret generic harbor-staging-admin \
    --from-literal=HARBOR_ADMIN_PASSWORD="${admin}"
fi

ADMIN_PASS="$(kretry kubectl -n "${NS}" get secret harbor-staging-admin \
  -o jsonpath='{.data.HARBOR_ADMIN_PASSWORD}' | base64 -d)"

kretry kubectl apply -f "${HERE}/minio.yaml"
kretry kubectl -n "${NS}" rollout status deploy/minio --timeout=180s
kretry kubectl -n "${NS}" wait --for=condition=complete job/minio-make-bucket --timeout=180s

helm repo add harbor https://helm.goharbor.io >/dev/null 2>&1 || true
helm repo update harbor >/dev/null

kretry helm upgrade --install "${RELEASE}" "${CHART}" \
  --version "${CHART_VERSION}" \
  --namespace "${NS}" \
  --create-namespace \
  -f "${HERE}/values.yaml" \
  --set harborAdminPassword="${ADMIN_PASS}" \
  --wait \
  --timeout 10m

echo
echo "installed ${RELEASE} in namespace ${NS}"
echo "admin password is in secret ${NS}/harbor-staging-admin (key HARBOR_ADMIN_PASSWORD)"
echo "reach it with: kubectl -n ${NS} port-forward svc/harbor 18080:80"
echo "do not helm upgrade -n harbor"
