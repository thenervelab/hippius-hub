#!/bin/sh
# Mirror one public Hugging Face repo onto your Hippius namespace.
# Works on macOS and Linux (POSIX sh, no macOS-only commands).
#
#   pip install -U "huggingface_hub[cli]" hippius_hub
#   hippius-hub login --hippius-token <token-from-console.hippius.com>
#   sh mirror-hf.sh org/model my-namespace/model
#
# The copy is published under the namespace you already have, public or private.
# Load it by changing the import and the repo id:
#   from hippius_hub import snapshot_download
#   snapshot_download("my-namespace/model")

set -eu

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: mirror-hf.sh <hf-org/repo> <namespace/repo> [revision]" >&2
  exit 2
fi

hf_repo=$1
hippius_repo=$2
revision=${3:-main}
name=${hf_repo##*/}

case $hf_repo in
  */*) ;;
  *) echo "hf repo must look like org/name" >&2; exit 2 ;;
esac
case $hippius_repo in
  */*) ;;
  *) echo "hippius repo must look like namespace/name" >&2; exit 2 ;;
esac
if [ -z "$name" ] || [ "$name" = "." ] || [ "$name" = ".." ]; then
  echo "hf repo name is empty" >&2
  exit 2
fi

for cmd in hf hippius-hub; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing '$cmd'. Install with: pip install -U \"huggingface_hub[cli]\" hippius_hub" >&2
    exit 1
  fi
done

dir="./${name}"

hf download "$hf_repo" --local-dir "$dir"
# hf leaves a download sidecar here. It is not part of the model.
rm -rf "$dir/.cache"
hippius-hub upload "$hippius_repo" "$dir" --revision "$revision"
