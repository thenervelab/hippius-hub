#!/bin/sh
# Copy Hugging Face models into a public Hippius namespace and a private one.
# Works on macOS and Linux (POSIX sh, no macOS-only commands).
#
#   pip install "hippius_hub>=0.7" "huggingface_hub>=1,<2"
#   hf auth login
#   hippius-hub login --hippius-token <token-from-console.hippius.com>
#   sh mirror-hf-namespaces.sh --public my-models
#   sh mirror-hf-namespaces.sh --public my-models --private my-models-private
#   sh mirror-hf-namespaces.sh --private my-models-private
#
# Visibility belongs to the namespace. Public models go to --public. Private
# models go to --private. If you omit --private, and the account has private
# models, the script asks before it creates <namespace>-private for them.
# Answering no copies the public models only.
#
# Each namespace has its own registry login. Logins are saved under
# ~/.cache/hippius/hub/robots/<namespace> and swapped in only while that
# namespace is being copied. The login that was active at the start is
# restored at the end. Datasets, Spaces, and buckets are skipped.

set -eu

usage() {
  echo "usage: mirror-hf-namespaces.sh --public <namespace> [--private <namespace>]" >&2
  echo "       mirror-hf-namespaces.sh --private <namespace>" >&2
  exit 2
}

valid_ns() {
  case $1 in
    ""|*"/"*|*" "*|*[!A-Za-z0-9._-]*)
      return 1
      ;;
  esac
  case $1 in
    [A-Za-z0-9]*) return 0 ;;
  esac
  return 1
}

robot_matches() {
  robot_user=
  [ -f "$1" ] || return 1
  line=$(cat "$1") || return 1
  case $line in
    "Basic "*) ;;
    *) return 1 ;;
  esac
  b64=${line#Basic }
  decoded=$(printf '%s' "$b64" | openssl base64 -d -A 2>/dev/null) || return 1
  robot_user=${decoded%%:*}
  case $robot_user in
    robot\$$2+*) return 0 ;;
  esac
  return 1
}

read_me() {
  me_out=$(hippius-hub registry me) || {
    echo "Could not read the active project from 'hippius-hub registry me'." >&2
    exit 1
  }
  me_project=$(printf '%s\n' "$me_out" | awk '/^Project:/ { print $2; exit }')
  me_public=$(printf '%s\n' "$me_out" | awk '/^Public:/ { print $2; exit }')
}

show_provlog() {
  [ -n "$provlog" ] && [ -f "$provlog" ] || return 0
  grep -v 'Secret:' "$provlog" >&2 || true
}

restore_login() {
  [ "$swapped" = 1 ] || return 0
  if [ "$had_token" = 1 ]; then
    cp "$orig_token" "$token_path"
    chmod 600 "$token_path"
  else
    rm -f "$token_path"
  fi
}

cleanup() {
  restore_login
  if [ -n "$work_dir" ] && [ -d "$work_dir" ]; then
    rm -rf "$work_dir"
  fi
  [ -n "$provlog" ] && rm -f "$provlog"
  [ -n "$list" ] && rm -f "$list"
  [ -n "$pubf" ] && rm -f "$privf" "$pubf"
  [ -n "$token_before" ] && rm -f "$token_before"
  [ -n "$orig_token" ] && rm -f "$orig_token"
}

wait_active() {
  i=0
  while [ "$i" -lt 15 ]; do
    if hippius-hub registry status | awk -v n="$1" '$1 == n && index($0, "status=active") { found = 1 } END { exit !found }'; then
      return 0
    fi
    i=$((i + 1))
    if [ "$i" -lt 15 ]; then
      sleep 2
    fi
  done
  return 1
}

stash_login() {
  cp "$1" "$stash_dir/$2"
  chmod 600 "$stash_dir/$2"
}

# Provision $1 if needed and remember its registry login. Sets last_created
# to 1 when this call created the namespace. Never prints the robot secret.
ensure_robot() {
  ns=$1
  last_created=0
  if [ -f "$token_path" ]; then
    cp "$token_path" "$token_before"
  else
    : > "$token_before"
  fi
  if ! hippius-hub registry provision "$ns" >"$provlog" 2>&1; then
    show_provlog
    exit 1
  fi
  if grep -q "already exists" "$provlog"; then
    last_created=0
  elif grep -q "Created " "$provlog"; then
    last_created=1
  elif grep -q "still being created" "$provlog" || grep -q "Provisioning started" "$provlog"; then
    if ! wait_active "$ns"; then
      echo "Namespace $ns is still being created. Check 'hippius-hub registry status' and rerun." >&2
      exit 1
    fi
    last_created=1
  else
    echo "Could not provision $ns." >&2
    show_provlog
    exit 1
  fi
  : > "$provlog"

  if [ -f "$token_path" ] && ! cmp -s "$token_before" "$token_path"; then
    if ! robot_matches "$token_path" "$ns"; then
      echo "The login saved by provision is not for $ns. Stopped before copying." >&2
      exit 1
    fi
    stash_login "$token_path" "$ns"
    return 0
  fi
  if robot_matches "$token_path" "$ns"; then
    stash_login "$token_path" "$ns"
    return 0
  fi
  if robot_matches "$stash_dir/$ns" "$ns"; then
    return 0
  fi
  echo "No registry login is saved for $ns on this machine." >&2
  echo "If 'hippius-hub registry me' shows $ns, run 'hippius-hub registry rotate-token' and rerun." >&2
  exit 1
}

# Set the active project's visibility. Refuses to change a different project.
# A private namespace that cannot be set private stops the script before any copy.
set_visibility() {
  ns=$1
  want=$2
  created=$3
  if [ "$want" = "public" ]; then
    want_flag=True
  else
    want_flag=False
  fi
  read_me
  if [ "$me_project" = "$ns" ]; then
    if [ "$me_public" != "$want_flag" ]; then
      if [ "$created" != 1 ]; then
        echo "Setting $ns to $want. Every repo already in that namespace changes with it."
      else
        echo "Setting $ns to $want."
      fi
      hippius-hub registry publicity "$want"
      read_me
      if [ "$me_project" != "$ns" ] || [ "$me_public" != "$want_flag" ]; then
        echo "Could not set $ns to $want. Stopped before copying." >&2
        exit 1
      fi
    fi
    return 0
  fi

  if [ "$want" = "public" ] && [ "$created" = 1 ]; then
    echo "$ns is new, so it is public."
    return 0
  fi
  if [ "$want" = "private" ]; then
    echo "Cannot set $ns private while the active project is ${me_project:-none}." >&2
    echo "Stopped before copying. Make $ns the active project, then rerun:" >&2
    echo "  sh mirror-hf-namespaces.sh --private $ns" >&2
    exit 1
  fi
  echo "Left the visibility of $ns unchanged. The active project is ${me_project:-none}."
}

confirm_private() {
  read_me
  if [ "$me_project" != "$1" ] || [ "$me_public" != "False" ]; then
    echo "Cannot confirm $1 is private. The active project is ${me_project:-none}. Stopped before copying." >&2
    exit 1
  fi
}

activate_ns() {
  if ! robot_matches "$stash_dir/$1" "$1"; then
    echo "Saved login for $1 is missing." >&2
    exit 1
  fi
  cp "$stash_dir/$1" "$token_path"
  chmod 600 "$token_path"
}

copy_list() {
  ns=$1
  file=$2
  activate_ns "$ns"
  while IFS= read -r repo_id; do
    [ -n "$repo_id" ] || continue
    name=${repo_id##*/}
    if [ -z "$name" ] || [ "$name" = "." ] || [ "$name" = ".." ]; then
      echo "skip $repo_id (empty name)" >&2
      continue
    fi
    echo "copy $repo_id -> $ns/$name"
    work_dir=$(mktemp -d)
    hf download "$repo_id" --local-dir "$work_dir/files"
    rm -rf "$work_dir/files/.cache"
    hippius-hub upload "$ns/$name" "$work_dir/files"
    rm -rf "$work_dir"
    work_dir=
  done < "$file"
}

public_ns=
private_ns=
while [ "$#" -gt 0 ]; do
  case $1 in
    --public)
      [ "$#" -ge 2 ] || usage
      public_ns=$2
      shift 2
      ;;
    --private)
      [ "$#" -ge 2 ] || usage
      private_ns=$2
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$public_ns" ] || [ -n "$private_ns" ] || usage
if [ -n "$public_ns" ] && ! valid_ns "$public_ns"; then
  echo "public namespace must be one name, such as my-models" >&2
  exit 2
fi
if [ -n "$private_ns" ] && ! valid_ns "$private_ns"; then
  echo "private namespace must be one name, such as my-models-private" >&2
  exit 2
fi
if [ -n "$public_ns" ] && [ "$public_ns" = "$private_ns" ]; then
  echo "public and private namespaces must be different names" >&2
  exit 2
fi

for cmd in hf hippius-hub openssl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing '$cmd'" >&2
    exit 1
  fi
done

swapped=0
had_token=0
work_dir=
provlog=
list=
pubf=
privf=
token_before=
orig_token=
last_created=0
me_project=
me_public=
cache=${HOME}/.cache/hippius/hub
token_path=$cache/token
stash_dir=$cache/robots

list=$(mktemp)
pubf=$(mktemp)
privf=$(mktemp)
provlog=$(mktemp)
token_before=$(mktemp)
orig_token=$(mktemp)
chmod 600 "$provlog" "$token_before" "$orig_token"
trap cleanup EXIT

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

do_public=0
do_private=0
[ -n "$public_ns" ] && do_public=1
[ -n "$private_ns" ] && do_private=1

if [ "$do_public" = 1 ] && [ -z "$private_ns" ] && [ "$priv_n" -gt 0 ]; then
  if [ ! -t 0 ]; then
    echo "You have $priv_n private models. Copying them needs a private namespace." >&2
    echo "Rerun in a terminal, or:" >&2
    echo "  sh mirror-hf-namespaces.sh --public $public_ns --private ${public_ns}-private" >&2
    exit 2
  fi
  printf 'You have %s private models. They will be copied to %s-private, a new private namespace. Copy them? [y/N] ' "$priv_n" "$public_ns" >&2
  read -r answer || answer=
  case $answer in
    y|Y|yes|YES)
      private_ns=${public_ns}-private
      if ! valid_ns "$private_ns"; then
        echo "derived namespace $private_ns is not a valid name" >&2
        exit 2
      fi
      do_private=1
      ;;
    *)
      echo "Leaving $priv_n private models on Hugging Face."
      ;;
  esac
fi

if [ "$do_public" = 1 ] && [ "$pub_n" -eq 0 ]; then
  echo "No public models to copy."
  do_public=0
fi
if [ "$do_private" = 1 ] && [ "$priv_n" -eq 0 ]; then
  echo "No private models to copy."
  do_private=0
fi
if [ "$do_public" = 0 ] && [ "$do_private" = 0 ]; then
  exit 0
fi

mkdir -p "$stash_dir"
chmod 700 "$stash_dir"
if [ -f "$token_path" ]; then
  cp "$token_path" "$orig_token"
  had_token=1
fi
swapped=1

private_created=0
public_created=0
if [ "$do_private" = 1 ]; then
  ensure_robot "$private_ns"
  private_created=$last_created
  set_visibility "$private_ns" private "$private_created"
  confirm_private "$private_ns"
  copy_list "$private_ns" "$privf"
  echo "Copied $priv_n private models to $private_ns."
fi
if [ "$do_public" = 1 ]; then
  ensure_robot "$public_ns"
  public_created=$last_created
  set_visibility "$public_ns" public "$public_created"
  copy_list "$public_ns" "$pubf"
  echo "Copied $pub_n public models to $public_ns."
fi
echo "Registry logins are saved in ~/.cache/hippius/hub/robots/."
if [ "$had_token" = 1 ]; then
  echo "Restored your previous registry login."
fi
