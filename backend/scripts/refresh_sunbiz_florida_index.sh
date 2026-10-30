#!/usr/bin/env bash
# Rebuild the Florida Sunbiz index and replace the `sunbiz-florida-index`
# release asset that production downloads at boot (the Florida wiring,
# 30 Oct 2026).
#
#   backend/scripts/refresh_sunbiz_florida_index.sh            # build + upload
#   backend/scripts/refresh_sunbiz_florida_index.sh --no-upload # build only
#
# The fallback for refresh-sunbiz-florida-index.yml: run it on a machine the
# Division's SFTP server answers (a home connection did on 29 Oct 2026) if a
# GitHub runner cannot reach sftp.floridados.gov:22. Needs uv (the builder
# runs with paramiko and zipfile-deflate64 brought in by `uv run --with`),
# ~4 GB of free disk, and, to upload, the `gh` CLI logged in to an account
# that can write releases on StephenAbbott/opencheck.
#
# macOS bash 3.2-safe: no heredoc inside $( ), no mapfile, no ${x,,}.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=${GITHUB_REPOSITORY:-StephenAbbott/opencheck}
tag=sunbiz-florida-index
asset=sunbiz_florida.sqlite.gz
upload=1
if [[ "${1:-}" == "--no-upload" ]]; then
  upload=0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/sunbiz-fl.XXXXXX")
echo "building in $work"

# Exit 2 = the builder refused its input (a missing member, a short file, a
# changed layout): nothing is uploaded and the release keeps the last index.
(cd "$here/.." && uv run --with paramiko --with zipfile-deflate64 \
  python scripts/build_sunbiz_florida_index.py --download --work "$work" \
  --out "$work/sunbiz_florida.sqlite" --gzip)

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
  gh release create "$tag" --repo "$repo" --title "Sunbiz Florida index" \
    --notes "Built quarterly from the Florida Department of State, Division of Corporations' bulk data downloads (Florida public records). Downloaded by the backend at boot (SUNBIZ_FLORIDA_DB_URL)."
fi

GITHUB_REPOSITORY="$repo" "$here/replace_release_asset.sh" "$tag" "$work/$asset" "$asset"
echo "uploaded $asset to $repo release $tag; production picks it up on its next boot"
rm -f "$work/cordata.zip" "$work/corevent.zip"
