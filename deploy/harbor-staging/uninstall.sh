#!/usr/bin/env bash
# Tear down harbor-staging only. Refuses namespace harbor (prod).
set -euo pipefail

NS=harbor-staging
RELEASE=harbor-staging

if [[ "${NS}" == "harbor" ]] || [[ "${HELM_NAMESPACE:-}" == "harbor" ]]; then
  echo "refusing to operate on namespace harbor (production)" >&2
  exit 2
fi

export KUBECONFIG="${KUBECONFIG:-${HOME}/Hippius-Storage/Configs/k8s/hippius.yaml}"

echo "prod helm (must not change):"
helm list -n harbor || true

if helm list -n "${NS}" -q 2>/dev/null | grep -qx "${RELEASE}"; then
  helm uninstall "${RELEASE}" --namespace "${NS}" --wait --timeout 5m
fi

kubectl delete job,deploy,svc,cm,secret,pvc --all -n "${NS}" --ignore-not-found --timeout=120s || true
kubectl delete ns "${NS}" --timeout=180s

echo "removed namespace ${NS}"
echo "prod helm:"
helm list -n harbor
