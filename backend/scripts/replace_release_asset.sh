#!/usr/bin/env bash
# Replace one asset of a GitHub release without ever leaving the release
# without it (Phase 302).
#
#   replace_release_asset.sh TAG FILE ASSET_NAME [NOTES]
#
# `gh release upload --clobber` deletes the old asset *before* uploading the
# new one and retries only the upload, so a delete that errors fails the job
# and an upload that fails after the delete leaves the release empty — and
# every backend boot that downloads it with nothing to fetch. This script
# instead:
#
#   1. uploads FILE as ASSET_NAME.new (clobbering any staging copy a failed
#      run left behind) and checks the staged size matches FILE;
#   2. deletes the old ASSET_NAME;
#   3. renames ASSET_NAME.new to ASSET_NAME;
#   4. sets the release notes, when given.
#
# Every call is retried with a growing back-off, and steps 2 and 3 are
# idempotent — a delete or rename that succeeded on GitHub's side but lost
# its response is recognised on the next attempt rather than failing. Until
# step 2 the old asset is untouched; the only window without ASSET_NAME is
# the seconds between steps 2 and 3. Needs GH_TOKEN and GITHUB_REPOSITORY.
# RETRY_ATTEMPTS (5) and RETRY_SLEEP_S (20; the nth retry waits n times it)
# exist for the tests.
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 TAG FILE ASSET_NAME [NOTES]" >&2
  exit 2
fi
tag=$1
file=$2
name=$3
notes=${4:-}
staging="${name}.new"
repo=${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}
attempts=${RETRY_ATTEMPTS:-5}
sleep_s=${RETRY_SLEEP_S:-20}

retry() {
  local i
  for ((i = 1; i <= attempts; i++)); do
    if "$@"; then
      return 0
    fi
    if ((i < attempts)); then
      echo "::warning::attempt $i of $attempts failed: $*"
      sleep $((sleep_s * i))
    fi
  done
  echo "::error::gave up after $attempts attempts: $*"
  return 1
}

# The id (or another field) of the release's asset called $1; empty when
# there is none. Fails only when the API call itself fails.
asset_field() {
  gh api "repos/${repo}/releases/tags/${tag}" \
    --jq ".assets[] | select(.name==\"$1\") | .$2"
}

upload_staging() {
  gh release upload "$tag" "$stage_path" --clobber
}

check_staging() {
  local size
  size=$(asset_field "$staging" size) || return 1
  if [[ "$size" != "$want_size" ]]; then
    echo "staged ${staging} is ${size:-missing} bytes; expected ${want_size}"
    return 1
  fi
}

delete_old() {
  local id
  id=$(asset_field "$name" id) || return 1
  [[ -z "$id" ]] && return 0 # already gone (a lost response last time)
  gh api -X DELETE "repos/${repo}/releases/assets/${id}"
}

rename_staging() {
  local id
  id=$(asset_field "$name" id) || return 1
  [[ -n "$id" ]] && return 0 # already renamed (a lost response last time)
  id=$(asset_field "$staging" id) || return 1
  if [[ -z "$id" ]]; then
    echo "neither ${name} nor ${staging} is on the release"
    return 1
  fi
  gh api -X PATCH "repos/${repo}/releases/assets/${id}" -f name="$name" >/dev/null
}

set_notes() {
  gh release edit "$tag" --notes "$notes"
}

want_size=$(wc -c <"$file" | tr -d ' ')
stage_dir=$(mktemp -d)
trap 'rm -rf "$stage_dir"' EXIT
stage_path="${stage_dir}/${staging}"
# A hard link when FILE is on the same filesystem (the asset is ~1 GB).
ln "$file" "$stage_path" 2>/dev/null || cp "$file" "$stage_path"

retry upload_staging
# A truncated upload must never replace a good asset: stop here, old intact.
retry check_staging
retry delete_old
retry rename_staging
if [[ -n "$notes" ]]; then
  retry set_notes
fi
echo "replaced ${name} on release ${tag} (${want_size} bytes)"
