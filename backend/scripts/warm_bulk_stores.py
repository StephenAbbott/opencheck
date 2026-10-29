#!/usr/bin/env python
"""Download the bulk stores the index-tier probes read, before the sweep runs.

``onrc_romania`` and ``meip`` answer from a local SQLite store that is a build
artifact, not a repo file — both gitignored, both published as GitHub release
assets, both downloaded at boot in production by the very functions this script
calls. On a fresh CI checkout they are absent, so both probes skipped **every
week** since they shipped. That was honest — the report said "not tested" — but
it meant ``expect_liveness={"snapshot"}`` on each of them, the one assertion
the sweep exists to make, was evaluated by nobody.

The sweep runs on a GitHub runner and the assets are on GitHub's own CDN
(1.3 MB gzipped for the ONRC index, 29 MB for MEIP), so this is one step.
``chilecompra`` (Phase 280), ``jar_lithuania`` (Phase 325) and ``ariregister`` (Phase 330) joined later on
the same terms; the JAR index in particular can be read from a runner only as
this asset, because the register's own files are challenged there.

``asp_moldova`` is the one store here that is not a release asset. It builds
itself from the register's weekly XLSX on dataset.gov.md (about 39 MB), and
until it was warmed here it built inside the sweep, where ``fetch()`` waits at
most 45 s for the background download and build before degrading. On a
GitHub runner that race was lost on 21 and 28 Sept 2026 — both runs degraded
at ~45.3 s with "the weekly export is still being downloaded and indexed" and
a stub liveness — and won on 5 Oct, so the row's colour said more about the
runner's route to Chişinău than about the register. Built here, with no clock,
the probe then finds a fresh index (``sync_index`` keeps a snapshot under
seven days old) and the 45 s wait it still exercises is the warm path. It
needs ``OPENCHECK_ALLOW_LIVE`` — ``sync_enabled()`` reads it — which the
workflow sets on this step; without it the outcome is ``not_configured``, no
file is written, and the warning below names it. Its file lives where
``asp_moldova.index_path()`` says (the temp directory unless
``ASP_MOLDOVA_DB_FILE`` is set), not under ``data_root()``: both steps run on
one runner, so the sweep's process resolves the same path.

Deliberately the production path, not a ``curl``
------------------------------------------------

``warm_index()`` and ``warm_meip_db()`` resolve the destination through each
module's own ``db_path()``, which is where ``requires_files`` looks — the two
mechanisms already agree about the path and a second one would be a third
opinion. Calling them also means this step exercises the code Render runs at
boot, so a broken asset URL surfaces here on a Monday rather than on a deploy.

Never fatal in itself: both functions swallow their own failures, because a
missing index is a state those modules model rather than a crash. This script
exits non-zero when one of them reports a failure so the step is visibly
amber in the workflow log, and the workflow lets it continue — a mirror blip
must not fail a sweep whose subject is the registers. The probe then skips
exactly as before, and the report names the assertion it could not evaluate.
"""

from __future__ import annotations

import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from opencheck.cache import data_root  # noqa: E402
from opencheck.sources.ariregister import db_path as ariregister_db_path  # noqa: E402
from opencheck.sources.ariregister import warm_index as warm_ariregister_index  # noqa: E402
from opencheck.sources.asp_moldova import index_path as asp_moldova_index_path  # noqa: E402
from opencheck.sources.asp_moldova import warm_index as warm_asp_moldova_index  # noqa: E402
from opencheck.sources.chilecompra import db_path as chilecompra_db_path  # noqa: E402
from opencheck.sources.chilecompra import warm_index as warm_chilecompra_index  # noqa: E402
from opencheck.meip import db_path as meip_db_path  # noqa: E402
from opencheck.meip import warm_meip_db  # noqa: E402
from opencheck.sources.jar_lithuania import db_path as jar_db_path  # noqa: E402
from opencheck.sources.jar_lithuania import warm_index as warm_jar_index  # noqa: E402
from opencheck.sources.bce_belgium import db_path as bce_db_path  # noqa: E402
from opencheck.sources.bce_belgium import warm_index as warm_bce_index  # noqa: E402
from opencheck.sources.edr_ukraine import db_path as edr_db_path  # noqa: E402
from opencheck.sources.edr_ukraine import warm_index as warm_edr_index  # noqa: E402
from opencheck.sources.cyprus_drcor import db_path as cyprus_db_path  # noqa: E402
from opencheck.sources.cyprus_drcor import warm_index as warm_cyprus_index  # noqa: E402
from opencheck.sources.onrc_romania import db_path as onrc_db_path  # noqa: E402
from opencheck.sources.onrc_romania import warm_index as warm_onrc_index  # noqa: E402

WARMERS = (
    ("onrc_romania", warm_onrc_index, onrc_db_path),
    ("meip", warm_meip_db, meip_db_path),
    ("chilecompra", warm_chilecompra_index, chilecompra_db_path),
    ("jar_lithuania", warm_jar_index, jar_db_path),
    ("bce_belgium", warm_bce_index, bce_db_path),
    ("edr_ukraine", warm_edr_index, edr_db_path),
    ("cyprus_drcor", warm_cyprus_index, cyprus_db_path),
    ("ariregister", warm_ariregister_index, ariregister_db_path),
    ("asp_moldova", warm_asp_moldova_index, asp_moldova_index_path),
)


def main() -> int:
    print(f"data root: {data_root()}")
    failed: list[str] = []
    for name, warm, path_of in WARMERS:
        outcome = warm().get(name, "")
        path = path_of()
        size = path.stat().st_size if path.exists() else 0
        print(f"{name}: {outcome} [{size:,} bytes on disk]")
        # Both conditions, because "did not fail" and "there is a file" are
        # different claims and only the second is what the probe needs.
        if str(outcome).startswith("failed") or size == 0:
            failed.append(name)

    if failed:
        print(
            f"::warning::could not warm {', '.join(failed)} — their probes will "
            "skip, and the report will say which assertion went unevaluated",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
