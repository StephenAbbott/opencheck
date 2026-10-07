"""Phase 302: the release-asset swap in ``scripts/replace_release_asset.sh``.

The script is run for real (bash) against a fake ``gh`` that keeps the
release's assets in a JSON file and can be told to fail any call — outright,
or after doing the work and losing the response, the case that makes a naive
retry delete the new asset or fail on a rename that already happened.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "replace_release_asset.sh"

FAKE_GH = r'''
import json, os, re, sys
state_path = os.environ["FAKE_GH_STATE"]
st = json.load(open(state_path))
args = sys.argv[1:]
st["calls"].append(" ".join(args))

def op_for(a):
    if a[:2] == ["release", "upload"]: return "upload"
    if a[:2] == ["release", "edit"]: return "edit"
    if a[0] == "api" and "-X" in a: return {"DELETE": "delete", "PATCH": "rename"}[a[a.index("-X") + 1]]
    return "read"

op = op_for(args)
plan = st["fail"].get(op)            # e.g. {"mode": "error"|"lost", "times": 1}
failing = bool(plan) and plan["times"] > 0
if failing:
    plan["times"] -= 1
mode = plan["mode"] if failing else None

def save():
    json.dump(st, open(state_path, "w"))

if mode == "error":
    save(); print(f"HTTP 502 on {op}", file=sys.stderr); sys.exit(1)

assets = st["assets"]
out = ""
if op == "upload":
    path = args[3]
    name = os.path.basename(path)
    size = os.path.getsize(path) - st.get("truncate", 0)
    assets[:] = [x for x in assets if x["name"] != name]
    st["next_id"] += 1
    assets.append({"id": st["next_id"], "name": name, "size": size})
elif op == "read":
    m = re.search(r'select\(\.name=="([^"]+)"\) \| \.(\w+)', args[args.index("--jq") + 1])
    out = "".join(str(x[m.group(2)]) + "\n" for x in assets if x["name"] == m.group(1))
elif op == "delete":
    aid = int(args[-1].rsplit("/", 1)[1])
    if not any(x["id"] == aid for x in assets):
        save(); print("HTTP 404", file=sys.stderr); sys.exit(1)
    assets[:] = [x for x in assets if x["id"] != aid]
elif op == "rename":
    aid = int(args[args.index("-X") + 2].rsplit("/", 1)[1])
    new = args[args.index("-f") + 1].split("=", 1)[1]
    if any(x["name"] == new for x in assets):
        save(); print("HTTP 422 already_exists", file=sys.stderr); sys.exit(1)
    for x in assets:
        if x["id"] == aid:
            x["name"] = new
elif op == "edit":
    st["notes"] = args[args.index("--notes") + 1]
save()
sys.stdout.write(out)
if mode == "lost":
    print(f"connection reset after {op}", file=sys.stderr); sys.exit(1)
'''


def _run(tmp_path: Path, *, fail: dict | None = None, truncate: int = 0, leftover: bool = False, notes: str = "built today"):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh = bindir / "gh"
    gh.write_text(f"#!{sys.executable}\n{FAKE_GH}")
    gh.chmod(0o755)
    assets = [{"id": 1, "name": "entity_pages.sqlite.gz", "size": 849}]
    if leftover:
        assets.append({"id": 2, "name": "entity_pages.sqlite.gz.new", "size": 3})
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"assets": assets, "next_id": 10, "fail": fail or {}, "calls": [], "truncate": truncate}))
    payload = tmp_path / "entity_pages.sqlite.gz"
    payload.write_bytes(b"x" * 1000)
    env = {
        **os.environ,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "FAKE_GH_STATE": str(state),
        "GITHUB_REPOSITORY": "StephenAbbott/opencheck",
        "RETRY_ATTEMPTS": "3",
        "RETRY_SLEEP_S": "0",
    }
    proc = subprocess.run(
        ["bash", str(SCRIPT), "entity-pages-latest", str(payload), "entity_pages.sqlite.gz", notes],
        env=env, capture_output=True, text=True, timeout=60,
    )
    return proc, json.loads(state.read_text())


def _names(st: dict) -> dict[str, int]:
    return {a["name"]: a["size"] for a in st["assets"]}


def test_happy_path_swaps_in_the_new_asset_and_sets_the_notes(tmp_path: Path) -> None:
    proc, st = _run(tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert _names(st) == {"entity_pages.sqlite.gz": 1000}
    assert st["notes"] == "built today"
    # The upload went to the staging name; the old asset was deleted only after.
    upload = next(i for i, c in enumerate(st["calls"]) if c.startswith("release upload"))
    delete = next(i for i, c in enumerate(st["calls"]) if "-X DELETE" in c)
    assert st["calls"][upload].endswith("entity_pages.sqlite.gz.new --clobber") and upload < delete


@pytest.mark.parametrize("op", ["read", "delete", "rename", "edit", "upload"])
def test_a_transient_error_on_any_call_is_retried(tmp_path: Path, op: str) -> None:
    """Run #5 on 7 Oct 2026: the step died before touching the asset, the
    shape of an unretried error on the lookup or the delete."""
    proc, st = _run(tmp_path, fail={op: {"mode": "error", "times": 1}})
    assert proc.returncode == 0, proc.stderr
    assert _names(st) == {"entity_pages.sqlite.gz": 1000}
    assert "attempt 1 of 3 failed" in proc.stdout


@pytest.mark.parametrize("op", ["delete", "rename"])
def test_a_call_that_succeeded_but_lost_its_response_is_recognised(tmp_path: Path, op: str) -> None:
    """A naive retry would delete the *new* asset, or fail renaming onto a
    name that already exists."""
    proc, st = _run(tmp_path, fail={op: {"mode": "lost", "times": 1}})
    assert proc.returncode == 0, proc.stderr
    assert _names(st) == {"entity_pages.sqlite.gz": 1000}


def test_an_upload_that_never_succeeds_leaves_the_old_asset_alone(tmp_path: Path) -> None:
    proc, st = _run(tmp_path, fail={"upload": {"mode": "error", "times": 99}})
    assert proc.returncode != 0
    assert _names(st) == {"entity_pages.sqlite.gz": 849}
    assert not any("-X DELETE" in c for c in st["calls"])
    assert "gave up after 3 attempts" in proc.stdout


def test_a_truncated_upload_never_replaces_a_good_asset(tmp_path: Path) -> None:
    proc, st = _run(tmp_path, truncate=10)
    assert proc.returncode != 0
    assert _names(st)["entity_pages.sqlite.gz"] == 849
    assert not any("-X DELETE" in c for c in st["calls"])
    assert "expected 1000" in proc.stdout


def test_a_staging_copy_left_by_a_failed_run_is_replaced(tmp_path: Path) -> None:
    proc, st = _run(tmp_path, leftover=True)
    assert proc.returncode == 0, proc.stderr
    assert _names(st) == {"entity_pages.sqlite.gz": 1000}


def test_the_refresh_workflow_uses_the_script() -> None:
    wf = (Path(__file__).resolve().parents[2] / ".github" / "workflows" / "refresh-entity-pages-db.yml").read_text()
    assert "backend/scripts/replace_release_asset.sh entity-pages-latest" in wf
    assert "gh release upload entity-pages-latest" not in wf
