#!/usr/bin/env bash
# Rebuild the Slovenian Business Register index from a CSV saved in a browser,
# and replace the `si-prs-index` release asset production downloads at boot.
#
#   backend/scripts/refresh_si_prs_index.sh ~/Downloads/opsiprs.csv 2026-11-01
#   backend/scripts/refresh_si_prs_index.sh ~/Downloads/opsiprs.csv 2026-11-01 --no-upload
#
# podatki.gov.si answers HTTP 407 to every client that is not a browser (the
# cloud, GitHub runners and curl on a home connection alike, 12 Nov 2026), so
# the file is downloaded by hand from
#   https://podatki.gov.si/dataset/poslovni-register-slovenije
# and the second argument is the "last modified" date shown there. The portal
# updates the file fortnightly; monthly is plenty — the probe flags an index
# older than 45 days, and every card shows the cut it was read from.
#
# Needs: python3 (standard library only) and, to upload, the `gh` CLI logged
# in to an account that can write releases on StephenAbbott/opencheck.
#
# macOS bash 3.2-safe: no heredoc inside $( ), no mapfile, no ${x,,}.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <opsiprs.csv> <cut YYYY-MM-DD> [--no-upload]" >&2
  exit 64
fi
csv=$1
cut=$2
upload=1
if [[ "${3:-}" == "--no-upload" ]]; then
  upload=0
fi

here=$(cd "$(dirname "$0")" && pwd)
repo=${GITHUB_REPOSITORY:-StephenAbbott/opencheck}
tag=si-prs-index
asset=si_prs.sqlite.gz

work=$(mktemp -d "${TMPDIR:-/tmp}/si-prs.XXXXXX")
echo "building in $work"

# Exit 2 = the builder refused its input (an error page, a changed header, a
# short file): nothing is uploaded and the release keeps the last index.
python3 -I "$here/build_si_prs_index.py" --csv "$csv" --cut "$cut" \
  --out "$work/si_prs.sqlite" --gzip

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
  gh release create "$tag" --repo "$repo" --title "Slovenia PRS index" \
    --notes "The Slovenian Business Register (PRS) index, built from AJPES's open file on podatki.gov.si (CC BY 4.0) by backend/scripts/refresh_si_prs_index.sh. Downloaded by the backend at boot (SI_PRS_DB_URL)."
fi

GITHUB_REPOSITORY="$repo" "$here/replace_release_asset.sh" "$tag" "$work/$asset" "$asset"
echo "uploaded $asset to $repo release $tag; production picks it up on its next boot"
echo "the build directory $work can be deleted"
