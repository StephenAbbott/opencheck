"""``backend/scripts/stamp_phase.py`` — phase numbers assigned at merge (Phase 347).

The script is imported by path so the file that ships is the file under test.
The fixtures below are the shapes of ``docs/status.md`` and ``README.md`` as
CLAUDE.md prescribes them; the real files are checked too, at the end, so a
hand edit that breaks the table fails here before the workflow sees it.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load():
    path = REPO / "backend" / "scripts" / "stamp_phase.py"
    spec = importlib.util.spec_from_file_location("_stamp_phase", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Dataclasses with postponed annotations resolve them through sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sp = _load()

STATUS = """# OpenCheck — Development History

OpenCheck has shipped through three hundred and forty-six phases. The latest commit on `main` is the source of truth.

| Phase | Headline |
|------:|----------|
| 0 | **Scaffold.** The first commit. Commit `0000000`. |
| 345 | **Anchors land.** The page scrolls itself. Commit `2c78a92`. |
| 346 | **OFAC's examples pin the rule.** Five structures. Commits `363e155`, `30dbcad`. |

Test suite: 7,263 backend tests pass (23 skipped — incl. the opt-in live smoke tier, 3 xfailed). Frontend type-checks clean and the vitest suite passes (1,283 tests across 102 files, incl. the Phase 345 anchor). A smoke runs too.
"""

README = """# OpenCheck

Intro paragraph.

## Status

**Latest: Phase 346** — OFAC's own FAQ examples pin the 50 Percent Rule arithmetic.

→ [Full development history](docs/status.md)

## Next section
"""

PENDING = """---
title: Phase numbers are assigned at merge.
readme: Phase numbers are now written when a pull request merges.
tests: backend +27, vitest +2, vitest_files +1
---
First paragraph of the narrative, with a | pipe in it.

Second paragraph, folded onto the same line.
"""


def _pending(text: str = PENDING, name: str = "stamping.md"):
    return sp.parse_pending(Path(name), text)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parse_reads_every_field_and_folds_the_body():
    p = _pending()
    assert p.title == "Phase numbers are assigned at merge."
    assert p.readme == "Phase numbers are now written when a pull request merges."
    assert p.tests == {"backend": 27, "vitest": 2, "vitest_files": 1}
    assert p.commits == []
    assert "\n" not in p.body
    assert p.body.startswith("First paragraph of the narrative, with a \\| pipe in it. Second paragraph")


def test_parse_accepts_quoted_values_and_explicit_commits():
    text = PENDING.replace(
        "title: Phase numbers are assigned at merge.",
        'title: "Phase numbers are assigned at merge."\ncommits: `3f1a2b4`, 9c8d7e6',
    )
    p = _pending(text)
    assert p.title == "Phase numbers are assigned at merge."
    assert p.commits == ["3f1a2b4", "9c8d7e6"]


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda t: t.replace("---\ntitle", "title", 1), "must start with"),
        (lambda t: t.replace("title: Phase numbers are assigned at merge.\n", ""), "'title' is required"),
        (lambda t: t.replace("at merge.", "at merge"), "must end with a full stop"),
        (lambda t: t.replace("readme: Phase numbers are now written when a pull request merges.\n", ""), "'readme' is required"),
        (lambda t: t.replace("merges.\n", "merges, see #412.\n"), "PR number"),
        (lambda t: t.replace("tests: backend +27", "tests: backend 27"), "'tests' must be like"),
        (lambda t: t.replace("tests: backend +27", "tests: e2e +1"), "'tests' must be like"),
        (lambda t: t.replace("tests:", "test:"), "unknown frontmatter key"),
        (lambda t: t.split("---\nFirst")[0] + "---\n", "narrative below the frontmatter is empty"),
        (lambda t: t + " Commit `abcdef0`.", "do not write the commit citation"),
        (lambda t: t.replace("merges.\n", "merges.\ncommits: not-a-sha\n"), "must be hex hashes"),
    ],
)
def test_parse_refuses_a_malformed_changeset(mutate, message):
    with pytest.raises(sp.PendingError, match=message):
        _pending(mutate(PENDING))


def test_load_pending_skips_the_readme(tmp_path):
    (tmp_path / "README.md").write_text("# not a changeset\n")
    (tmp_path / "b-second.md").write_text(PENDING)
    (tmp_path / "a-first.md").write_text(PENDING)
    assert [p.slug for p in sp.load_pending(tmp_path)] == ["a-first", "b-second"]


def test_load_pending_with_no_directory_is_empty(tmp_path):
    assert sp.load_pending(tmp_path / "missing") == []


# ---------------------------------------------------------------------------
# Number words
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "n, words",
    [
        (0, "zero"),
        (7, "seven"),
        (19, "nineteen"),
        (20, "twenty"),
        (46, "forty-six"),
        (100, "one hundred"),
        (101, "one hundred and one"),
        (137, "one hundred and thirty-seven"),
        (300, "three hundred"),
        (346, "three hundred and forty-six"),
        (347, "three hundred and forty-seven"),
        (999, "nine hundred and ninety-nine"),
        (1000, "one thousand"),
        (1005, "one thousand and five"),
        (1234, "one thousand two hundred and thirty-four"),
    ],
)
def test_number_words(n, words):
    assert sp.number_words(n) == words


def test_number_words_matches_every_intro_the_file_has_carried():
    # The intro sentence has been written by hand for 346 phases; the words the
    # script writes must be the words a reader of that file expects.
    assert sp.number_words(sp.highest_phase(STATUS)) == "three hundred and forty-six"


# ---------------------------------------------------------------------------
# status.md
# ---------------------------------------------------------------------------


def test_check_status_shape_accepts_the_prescribed_file():
    sp.check_status_shape(STATUS)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda s: s.replace("| 345 |", "\n| 345 |"), "blank lines"),
        (lambda s: s.replace("| 346 |", "Some heading\n| 346 |"), "non-row line sits between rows"),
        (lambda s: s.replace("| 345 |", "| 346 |"), "appear twice"),
        (lambda s: s.replace("| 345 |", "| 347 |"), "last row is phase 346 but the highest is 347"),
        (lambda s: s.replace("forty-six phases", "forty-five phases"), "intro says"),
    ],
)
def test_check_status_shape_catches_each_rule(mutate, message):
    with pytest.raises(sp.PendingError, match=message):
        sp.check_status_shape(mutate(STATUS))


def test_apply_appends_the_row_bumps_the_count_and_applies_the_deltas(capsys):
    out = sp.apply_to_status(STATUS, _pending(), 347, ["abc1234"])
    lines = out.splitlines()
    rows = [line for line in lines if sp.ROW_RE.match(line)]
    assert rows[-1] == (
        "| 347 | **Phase numbers are assigned at merge.** First paragraph of the narrative, "
        "with a \\| pipe in it. Second paragraph, folded onto the same line. Commit `abc1234`. |"
    )
    assert rows[-2].startswith("| 346 |")
    assert "three hundred and forty-seven phases" in out
    assert "7,290 backend tests pass" in out
    assert "(1,285 tests across 103 files" in out
    assert sum(1 for line in lines if line == "") == 3
    assert "7,263 → 7,290" in capsys.readouterr().out
    sp.check_status_shape(out)


def test_apply_with_no_tests_leaves_the_footer_alone():
    p = _pending(PENDING.replace("tests: backend +27, vitest +2, vitest_files +1\n", ""))
    out = sp.apply_to_status(STATUS, p, 347, ["abc1234"])
    assert "7,263 backend tests pass" in out
    assert "(1,283 tests across 102 files" in out


def test_apply_cites_several_commits_in_the_existing_form():
    out = sp.apply_to_status(STATUS, _pending(), 347, ["abc1234", "def5678"])
    assert out.splitlines()[-3].endswith("Commits `abc1234`, `def5678`. |")


def test_apply_refuses_a_number_that_is_not_the_next_one():
    with pytest.raises(sp.PendingError, match="asked for phase 348 but the last row is 346"):
        sp.apply_to_status(STATUS, _pending(), 348, ["abc1234"])


def test_apply_twice_numbers_consecutively():
    first = sp.apply_to_status(STATUS, _pending(), 347, ["abc1234"])
    second = sp.apply_to_status(first, _pending(name="other.md"), 348, ["bcd2345"])
    rows = [line for line in second.splitlines() if sp.ROW_RE.match(line)]
    assert [int(sp.ROW_RE.match(r).group(1)) for r in rows[-3:]] == [346, 347, 348]
    assert "three hundred and forty-eight phases" in second
    assert "7,317 backend tests pass" in second  # 7,263 + 27 + 27


# ---------------------------------------------------------------------------
# README
# ---------------------------------------------------------------------------


def test_readme_latest_line_is_replaced_and_nothing_else_moves():
    out = sp.apply_to_readme(README, _pending(), 347)
    assert "**Latest: Phase 347** — Phase numbers are now written when a pull request merges." in out
    assert "Phase 346" not in out
    assert out.count("**Latest:") == 1
    assert out.replace(
        "**Latest: Phase 347** — Phase numbers are now written when a pull request merges.",
        "**Latest: Phase 346** — OFAC's own FAQ examples pin the 50 Percent Rule arithmetic.",
    ) == README


def test_readme_without_a_latest_line_is_refused():
    with pytest.raises(sp.PendingError, match="Latest: Phase N"):
        sp.apply_to_readme("# OpenCheck\n", _pending(), 347)


# ---------------------------------------------------------------------------
# git: landing order and the cited commit
# ---------------------------------------------------------------------------


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
        env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x", "GIT_COMMITTER_NAME": "t",
             "GIT_COMMITTER_EMAIL": "t@x", "HOME": str(cwd), "PATH": "/usr/bin:/bin:/usr/local/bin"},
    ).stdout.strip()


def test_landing_commit_is_the_merge_that_brought_the_file_onto_main(tmp_path, monkeypatch):
    """Two branches, merged in the opposite order to their creation: the stamp
    orders them by when they LANDED and cites the merge commit, not the branch
    commit."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / "README.md").write_text("x\n")
    _git(repo, "add", "."); _git(repo, "commit", "-q", "-m", "init")
    pending = repo / ".phases" / "pending"
    pending.mkdir(parents=True)

    for name in ("alpha", "beta"):
        _git(repo, "checkout", "-q", "-b", name, "main")
        pending.mkdir(parents=True, exist_ok=True)
        (pending / f"{name}.md").write_text(PENDING)
        _git(repo, "add", "."); _git(repo, "commit", "-q", "-m", f"{name} work")
    _git(repo, "checkout", "-q", "main")
    _git(repo, "merge", "-q", "--no-ff", "-m", "Merge beta", "beta")
    _git(repo, "merge", "-q", "--no-ff", "-m", "Merge alpha", "alpha")

    monkeypatch.setattr(sp, "REPO", repo)
    monkeypatch.setattr(sp, "PENDING_DIR", pending)
    ordered = sp.ordered_by_landing(sp.load_pending(pending))
    assert [p.slug for p, _ in ordered] == ["beta", "alpha"]
    merge_beta = _git(repo, "log", "--format=%h", "-n", "1", "--grep=Merge beta")
    merge_alpha = _git(repo, "log", "--format=%h", "-n", "1", "--grep=Merge alpha")
    assert [c for _, c in ordered] == [[merge_beta], [merge_alpha]]


def test_explicit_commits_override_the_landing_commit(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    pending = repo / ".phases" / "pending"
    pending.mkdir(parents=True)
    (pending / "x.md").write_text(PENDING.replace("---\nFirst", "commits: 1234abc\n---\nFirst"))
    _git(repo, "add", "."); _git(repo, "commit", "-q", "-m", "x")
    monkeypatch.setattr(sp, "REPO", repo)
    assert sp.ordered_by_landing(sp.load_pending(pending))[0][1] == ["1234abc"]


# ---------------------------------------------------------------------------
# The real files, as they stand
# ---------------------------------------------------------------------------


def test_the_repository_status_file_obeys_the_rules():
    sp.check_status_shape((REPO / "docs" / "status.md").read_text(encoding="utf-8"))


def test_the_repository_readme_has_one_latest_line():
    text = (REPO / "README.md").read_text(encoding="utf-8")
    assert len(sp.README_LATEST_RE.findall(text)) == 1


def test_every_pending_changeset_in_the_repository_parses():
    for p in sp.load_pending():
        assert p.title and p.readme and p.body
