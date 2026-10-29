#!/usr/bin/env bash
# Rebuild the Ukrainian ЄДР index and replace the `edr-ukraine-index`
# release asset that production downloads at boot.
#
#   backend/scripts/refresh_edr_ukraine_index.sh               # download + build + upload
#   backend/scripts/refresh_edr_ukraine_index.sh --no-upload   # download + build only
#   backend/scripts/refresh_edr_ukraine_index.sh --zip UO.zip  # build from a file already downloaded
#
# Run it on a machine data.gov.ua answers: a home connection. The download
# sits behind a Cloudflare rule that refuses every datacentre address (HTTP
# 403 from the cloud and from a GitHub runner; a residential address gets the
# file), so this cannot be a GitHub Actions workflow — like the Lithuanian
# index, it is rebuilt by hand. The Ministry publishes weekly; the probe flags
# an index older than 28 days, and every card shows the export date it was
# read from, so every few weeks is plenty.
#
# Needs: the backend environment (`uv sync` in backend/ — the builder imports
# the adapter's parsers), ~4 GB of free space in $TMPDIR for the 328 MB ZIP and
# the ~1 GB index, and, to upload, the `gh` CLI logged in to an account that
# can write releases on StephenAbbott/opencheck. The upload goes through
# replace_release_asset.sh, which never leaves the release without the asset
# (Phase 302). Takes about ten minutes end to end.
#
# macOS bash 3.2-safe: no heredoc inside $( ), no mapfile, no ${x,,}.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=${GITHUB_REPOSITORY:-StephenAbbott/opencheck}
tag=edr-ukraine-index
asset=edr_ukraine.sqlite.gz
dataset_url="https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/d40cc921-39bb-44fd-be06-dc02589f45c6/download/uo.zip"
upload=1
zip=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-upload) upload=0; shift ;;
    --zip) zip="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

work=$(mktemp -d "${TMPDIR:-/tmp}/edr-ua.XXXXXX")
echo "building in $work"

if [[ -z "$zip" ]]; then
  zip="$work/UO.zip"
  echo "downloading UO.zip (about 330 MB)"
  # -C - resumes a partial download; a 403 here means a datacentre address.
  curl -fL -C - --retry 3 -o "$zip" "$dataset_url"
fi
if ! unzip -tq "$zip" >/dev/null 2>&1; then
  echo "$zip is not a ZIP — data.gov.ua answered with something else (a 403 page?)" >&2
  exit 2
fi

# The builder reads the file twice (dedupe pass, then write) — about five
# minutes — and refuses nothing silently: a jump in unparsed beneficiary
# strings is printed as a warning at the end. Read it before uploading.
(cd "$here/.." && uv run python scripts/build_edr_ukraine_index.py \
  --zip "$zip" --out "$work/edr_ukraine.sqlite" --gzip)

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
  gh release create "$tag" --repo "$repo" --title "Ukraine EDR index" \
    --notes "The Ukrainian ЄДР (Unified State Register of Legal Entities) index, built from the Ministry of Justice's weekly open-data export on data.gov.ua (CC BY 4.0) by backend/scripts/refresh_edr_ukraine_index.sh. Downloaded by the backend at boot (EDR_UKRAINE_DB_URL)."
fi

GITHUB_REPOSITORY="$repo" "$here/replace_release_asset.sh" "$tag" "$work/$asset" "$asset"
echo "uploaded $asset to $repo release $tag; production picks it up on its next boot"
echo "the build directory $work (ZIP and index, ~1.5 GB) can be deleted"
