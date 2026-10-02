#!/bin/sh
# Copy every model on the logged-in Hugging Face account into one Hippius namespace.
# Works on macOS and Linux (POSIX sh, no macOS-only commands).
#
#   pip install -U "huggingface_hub[cli]" hippius_hub
#   hf auth login
#   hippius-hub login --hippius-token <token-from-console.hippius.com>
#   sh mirror-hf-account.sh my-models
#
# Hippius visibility is the whole namespace, not each model. When every model
# is public, or every model is private, the namespace is set to match and every
# model is copied. When the account has both, pass public or private to copy
# only that group and set the namespace. Datasets, Spaces, and buckets are skipped.
#
#   sh mirror-hf-account.sh my-models private
#
# Load a copy by changing the import and the repo id:
#   from hippius_hub import snapshot_download
#   snapshot_download("my-models/model-name")

set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: mirror-hf-account.sh <namespace> [public|private]" >&2
  exit 2
fi

namespace=$1
only=${2:-}

case $namespace in
  ""|*"/"*|*" "*)
    echo "namespace must be a single name, not namespace/repo" >&2
    exit 2
    ;;
esac
case $only in
  ""|public|private) ;;
  *)
    echo "usage: mirror-hf-account.sh <namespace> [public|private]" >&2
    exit 2
    ;;
esac

for cmd in hf hippius-hub; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing '$cmd'. Install with: pip install -U \"huggingface_hub[cli]\" hippius_hub" >&2
    exit 1
  fi
done

list=$(mktemp)
pubf=$(mktemp)
privf=$(mktemp)
trap 'rm -f "$list" "$pubf" "$privf"' EXIT

hf repos ls --limit 0 --format agent > "$list"

while IFS="$(printf '\t')" read -r repo_id repo_type _updated visibility _rest; do
  if [ "$repo_id" = "id" ] || [ -z "$repo_id" ]; then
    continue
  fi
  if [ "$repo_id" = "No results found." ]; then
    echo "No Hugging Face models for the logged-in account."
    exit 0
  fi
  case $repo_type in
    model) ;;
    *)
      echo "skip $repo_id ($repo_type is not a model)"
      continue
      ;;
  esac
  case $visibility in
    public) printf '%s\n' "$repo_id" >> "$pubf" ;;
    private) printf '%s\n' "$repo_id" >> "$privf" ;;
    *)
      echo "skip $repo_id (visibility '$visibility' is not public or private)"
      ;;
  esac
done < "$list"

pub_n=$(grep -c . "$pubf" || true)
priv_n=$(grep -c . "$privf" || true)

if [ "$pub_n" -eq 0 ] && [ "$priv_n" -eq 0 ]; then
  echo "No Hugging Face models to copy."
  exit 0
fi

target=$only
if [ -z "$target" ]; then
  if [ "$pub_n" -gt 0 ] && [ "$priv_n" -gt 0 ]; then
    echo "Hippius visibility applies to the whole namespace, not each model." >&2
    echo "$pub_n public, $priv_n private." >&2
    echo "Re-run as: sh mirror-hf-account.sh $namespace public" >&2
    echo "       or: sh mirror-hf-account.sh $namespace private" >&2
    echo "That copies one group and sets the namespace. The other group stays on Hugging Face." >&2
    exit 2
  fi
  if [ "$pub_n" -gt 0 ]; then
    target=public
  else
    target=private
  fi
fi

if [ "$target" = "public" ]; then
  src=$pubf
  skip=$privf
  skip_n=$priv_n
else
  src=$privf
  skip=$pubf
  skip_n=$pub_n
fi

if [ "$skip_n" -gt 0 ]; then
  if [ "$target" = "public" ]; then
    other=private
  else
    other=public
  fi
  echo "Leaving these $other models on Hugging Face:"
  cat "$skip"
fi

if ! grep -q . "$src"; then
  echo "No $target models to copy."
  exit 0
fi

echo "Setting $namespace to $target. Every repo already in that namespace changes with it."
hippius-hub registry publicity "$target"

while IFS= read -r repo_id; do
  [ -n "$repo_id" ] || continue
  name=${repo_id##*/}
  if [ -z "$name" ] || [ "$name" = "." ] || [ "$name" = ".." ]; then
    echo "skip $repo_id (empty name)" >&2
    continue
  fi
  echo "copy $repo_id -> $namespace/$name"
  work=$(mktemp -d)
  hf download "$repo_id" --local-dir "$work/files"
  rm -rf "$work/files/.cache"
  hippius-hub upload "$namespace/$name" "$work/files"
  rm -rf "$work"
done < "$src"
