#!/usr/bin/env bash
# Rebuild the Bulgarian Commercial Register index and replace the
# `bg-tr-index` release asset that production downloads at boot.
#
#   backend/scripts/refresh_bg_tr_index.sh [DATA_DIR]            # fetch + build + upload
#   backend/scripts/refresh_bg_tr_index.sh [DATA_DIR] --no-upload # fetch + build only
#
# Run it with a Bulgarian VPN on: data.egov.bg answers 403 to every address
# outside Bulgaria, so this can never be a GitHub Actions workflow. DATA_DIR
# (default ~/opencheck-bg-data) keeps the daily files between runs, so a
# refresh only downloads the days added since the last one. The index is a
# replay of every file since 1 January 2021 and is rebuilt whole each time.
#
# Needs: python3 (standard library only) and, to upload, the `gh` CLI logged
# in to an account that can write releases on StephenAbbott/opencheck.
#
# macOS bash 3.2-safe: no heredoc inside $( ), no mapfile, no ${x,,}.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=${GITHUB_REPOSITORY:-StephenAbbott/opencheck}
tag=bg-tr-index
asset=bg_tr.sqlite.gz
data_dir="$HOME/opencheck-bg-data"
upload=1
for arg in "$@"; do
  if [[ "$arg" == "--no-upload" ]]; then
    upload=0
  else
    data_dir=$arg
  fi
done

echo "fetching new daily files into $data_dir"
python3 "$here/fetch_bg_tr.py" get --since 2021-01-01 --out "$data_dir"

# The fetcher writes one folder per dataset; the daily XML files are in the
# one holding the most .xml.gz files.
input_dir=""
best=0
for d in "$data_dir"/*/; do
  n=$(find "$d" -maxdepth 1 -name '*.xml.gz' | wc -l | tr -d ' ')
  if [[ "$n" -gt "$best" ]]; then
    best=$n
    input_dir=$d
  fi
done
if [[ -z "$input_dir" ]]; then
  echo "no daily files under $data_dir" >&2
  exit 1
fi
echo "building from $input_dir ($best files)"

# SQLite cannot write on some mounted folders: build in a local temp dir.
work=$(mktemp -d "${TMPDIR:-/tmp}/bg-tr.XXXXXX")
# Exit 2 = the builder refused its input: nothing is uploaded.
python3 "$here/build_bg_tr_index.py" --input-dir "$input_dir" --out "$work/bg_tr.sqlite" --gzip

size=$(wc -c <"$work/$asset" | tr -d ' ')
echo "built $work/$asset ($size bytes)"

if [[ "$upload" == "0" ]]; then
  echo "--no-upload: leaving $work/$asset in place"
  exit 0
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh is not installed; the index is at $work/$asset — upload it with:" >&2
  echo "  GITHUB_REPOSITORY=$repo $here/replace_release_asset.sh $tag $work/$asset $asset" >&2
  exit 1
fi

if ! gh release view "$tag" --repo "$repo" >/dev/null 2>&1; then
  echo "creating release $tag"
  gh release create "$tag" --repo "$repo" --title "Bulgaria Commercial Register index" \
    --notes "The Bulgarian Commercial Register index, a replay of the Registry Agency's daily open-data files since 1 January 2021 (data.egov.bg, CC0), built by backend/scripts/refresh_bg_tr_index.sh. Downloaded by the backend at boot (BG_TR_DB_URL)."
fi

GITHUB_REPOSITORY="$repo" "$here/replace_release_asset.sh" "$tag" "$work/$asset" "$asset"
echo "uploaded $asset to $repo release $tag; production picks it up on its next boot"
