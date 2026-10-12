#!/usr/bin/env bash
# Rebuild the Lithuanian JAR index and replace the `jar-lithuania-index`
# release asset that production downloads at boot (Phase 325).
#
#   backend/scripts/refresh_jar_lithuania_index.sh            # build + upload
#   backend/scripts/refresh_jar_lithuania_index.sh --no-upload # build only
#
# Run it on a machine Registrų centras answers: a home connection. Its files
# sit behind a Cloudflare challenge that every datacentre address receives,
# so this cannot be a GitHub Actions workflow (the runners are challenged
# too) — it is the one OpenCheck index rebuilt by hand. Weekly is plenty:
# the register publishes daily, the probe flags an index older than 21 days,
# and every card shows the cut it was read from.
#
# Needs: python3 (standard library only — no venv) and, to upload, the `gh`
# CLI logged in to an account that can write releases on
# StephenAbbott/opencheck. The upload goes through replace_release_asset.sh,
# which never leaves the release without the asset (Phase 302).
#
# macOS bash 3.2-safe: no heredoc inside $( ), no mapfile, no ${x,,}.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=${GITHUB_REPOSITORY:-StephenAbbott/opencheck}
tag=jar-lithuania-index
asset=jar_lithuania.sqlite.gz
upload=1
if [[ "${1:-}" == "--no-upload" ]]; then
  upload=0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/jar-lt.XXXXXX")
echo "building in $work"

# Exit 2 = the builder refused its input (a challenge page, a short or
# malformed file): nothing is uploaded and the release keeps last week's index.
python3 "$here/build_jar_lithuania_index.py" --out "$work/jar_lithuania.sqlite" --gzip

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
  gh release create "$tag" --repo "$repo" --title "Lithuania JAR index" \
    --notes "The Lithuanian Register of Legal Entities (JAR) index, built from Registrų centras' open data (CC BY 4.0) by backend/scripts/refresh_jar_lithuania_index.sh. Downloaded by the backend at boot (JAR_LITHUANIA_DB_URL)."
fi

GITHUB_REPOSITORY="$repo" "$here/replace_release_asset.sh" "$tag" "$work/$asset" "$asset"
echo "uploaded $asset to $repo release $tag; production picks it up on its next boot"
echo "the build directory $work can be deleted"
