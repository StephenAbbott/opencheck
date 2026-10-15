#!/usr/bin/env python3
"""Assign phase numbers at merge time, from pending changeset files.

Until Phase 347 every branch claimed its own phase number and edited the three
shared files — ``docs/status.md``, ``README.md`` and the generated
``frontend/src/lib/statusMarkdown.json`` — at the same spot (the end of the
table), so any two branches in flight at once collided on the number and
conflicted on the files. This script moves the stamp to the moment the work
lands on ``main``:

* A branch adds **one file** under ``.phases/pending/`` (format in the README
  there) and touches none of the three files.
* ``.github/workflows/stamp-phase.yml`` runs this script on every push to
  ``main`` that carries a pending file: it takes N = the highest row + 1, appends
  the row citing the commit that brought the file onto ``main``, bumps the
  spelled-out count in the intro sentence, applies any test-count deltas to the
  footer, rewrites the README's single ``Latest`` line, deletes the pending
  file and commits. ``gen-changelog.mjs`` regenerates the JSON in the same step.

Several pending files in one push are stamped in the order their commits
reached ``main`` (first-parent history), so a merge queue or two quick merges
number themselves correctly.

The rules of ``docs/status.md`` that CLAUDE.md spells out — one unbroken table,
no blank line between rows, every row ending in a commit citation, exactly three
blank lines in the file — are asserted here after every change, so a broken
table fails the workflow rather than reaching ``main``.

Usage::

    python3 backend/scripts/stamp_phase.py --check [--base origin/main]
        validate every pending file (a pull-request gate); with --base, also
        refuse a branch that both adds a pending file and hand-edits the
        stamped files
    python3 backend/scripts/stamp_phase.py --apply [--dry-run]
        stamp every pending file into docs/status.md and README.md and
        delete it; --dry-run prints what would change and writes nothing

Standard library only: the workflow runs it with the runner's ``python3``,
without installing the backend.

Exit status: 0 clean, 1 a pending file or the table is invalid, 2 usage.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PENDING_DIR = REPO / ".phases" / "pending"
STATUS_MD = REPO / "docs" / "status.md"
README_MD = REPO / "README.md"

ROW_RE = re.compile(r"^\| (\d+) \| (.*) \|$")
INTRO_RE = re.compile(r"^(OpenCheck has shipped through )([a-z\- ]+?)( phases\.)")
README_LATEST_RE = re.compile(r"^\*\*Latest: Phase \d+\*\* — .*$", re.MULTILINE)
CITATION_RE = re.compile(r"Commits? `[0-9a-f]{7,40}`(?:, `[0-9a-f]{7,40}`)*\.$")
BACKEND_COUNT_RE = re.compile(r"(\d[\d,]*) backend tests pass")
VITEST_COUNT_RE = re.compile(r"\((\d[\d,]*) tests across (\d+) files")
SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
TEST_KEYS = ("backend", "vitest", "vitest_files")
EXPECTED_BLANK_LINES = 3

# The three files a branch must no longer touch once it carries a pending file.
STAMPED_FILES = (
    "docs/status.md",
    "README.md",
    "frontend/src/lib/statusMarkdown.json",
)


class PendingError(ValueError):
    """A pending file, or the table, is not in the shape the stamp needs."""


@dataclass
class Pending:
    """One pending changeset, parsed."""

    path: Path
    title: str
    readme: str
    body: str
    tests: dict[str, int] = field(default_factory=dict)
    commits: list[str] = field(default_factory=list)

    @property
    def slug(self) -> str:
        return self.path.stem


# ---------------------------------------------------------------------------
# Parsing a pending file
# ---------------------------------------------------------------------------


def parse_pending(path: Path, text: str) -> Pending:
    """Parse one pending changeset.

    The file is a YAML-looking frontmatter — ``key: value`` lines between two
    ``---`` lines, no nesting — followed by the narrative. Only the keys below
    are read; anything else is an error, so a typo cannot silently drop a
    field.
    """
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise PendingError(f"{path.name}: must start with a '---' frontmatter line")
    try:
        end = next(i for i in range(1, len(lines)) if lines[i].strip() == "---")
    except StopIteration:
        raise PendingError(f"{path.name}: frontmatter is not closed with '---'") from None

    fields: dict[str, str] = {}
    for raw in lines[1:end]:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if ":" not in raw:
            raise PendingError(f"{path.name}: frontmatter line is not 'key: value': {raw!r}")
        key, _, value = raw.partition(":")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key in fields:
            raise PendingError(f"{path.name}: duplicate frontmatter key {key!r}")
        fields[key] = value

    unknown = set(fields) - {"title", "readme", "tests", "commits"}
    if unknown:
        raise PendingError(f"{path.name}: unknown frontmatter key(s): {', '.join(sorted(unknown))}")

    title = fields.get("title", "").strip()
    readme = fields.get("readme", "").strip()
    if not title:
        raise PendingError(f"{path.name}: 'title' is required")
    if title[-1] not in ".?!":
        raise PendingError(f"{path.name}: 'title' must end with a full stop — it is the bold headline")
    if not readme:
        raise PendingError(f"{path.name}: 'readme' is required (the README's one-line Latest summary)")
    if readme[-1] not in ".?!":
        raise PendingError(f"{path.name}: 'readme' must be one sentence ending in a full stop")
    if re.search(r"`[0-9a-f]{7,40}`|#\d+", readme):
        raise PendingError(f"{path.name}: 'readme' carries a commit hash or PR number; it is plain prose")

    body = _one_paragraph(lines[end + 1 :])
    if not body:
        raise PendingError(f"{path.name}: the narrative below the frontmatter is empty")
    if CITATION_RE.search(body):
        raise PendingError(f"{path.name}: do not write the commit citation yourself; the stamp adds it")

    tests = _parse_tests(path, fields.get("tests", ""))
    commits = _parse_commits(path, fields.get("commits", ""))
    return Pending(path=path, title=title, readme=readme, body=body, tests=tests, commits=commits)


def _one_paragraph(lines: list[str]) -> str:
    """Fold the narrative into one line: a row is one line, however long."""
    text = " ".join(part.strip() for part in lines if part.strip())
    text = re.sub(r"\s+", " ", text).strip()
    # A literal pipe would end the table cell; escape any the author left bare.
    return re.sub(r"(?<!\\)\|", r"\\|", text)


def _parse_tests(path: Path, value: str) -> dict[str, int]:
    """``tests: backend +12, vitest +3, vitest_files +1`` → deltas by key."""
    deltas: dict[str, int] = {}
    if not value:
        return deltas
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        m = re.fullmatch(r"([a-z_]+)\s+([+-]\d+)", part)
        if not m or m.group(1) not in TEST_KEYS:
            raise PendingError(
                f"{path.name}: 'tests' must be like 'backend +12, vitest +3, vitest_files +1'; got {part!r}"
            )
        deltas[m.group(1)] = int(m.group(2))
    return deltas


def _parse_commits(path: Path, value: str) -> list[str]:
    commits = [c.strip().strip("`") for c in value.split(",") if c.strip()]
    for c in commits:
        if not SHA_RE.match(c):
            raise PendingError(f"{path.name}: 'commits' must be hex hashes; got {c!r}")
    return commits


def load_pending(directory: Path = PENDING_DIR) -> list[Pending]:
    """Every ``*.md`` in the pending directory except its README, unordered."""
    if not directory.is_dir():
        return []
    out = []
    for path in sorted(directory.glob("*.md")):
        if path.name.lower() == "readme.md":
            continue
        out.append(parse_pending(path, path.read_text(encoding="utf-8")))
    return out


# ---------------------------------------------------------------------------
# The number words for the intro sentence
# ---------------------------------------------------------------------------

_ONES = (
    "zero one two three four five six seven eight nine ten eleven twelve "
    "thirteen fourteen fifteen sixteen seventeen eighteen nineteen"
).split()
_TENS = "zero ten twenty thirty forty fifty sixty seventy eighty ninety".split()


def number_words(n: int) -> str:
    """0–9999 in the British spelling the intro sentence has always used."""
    if not 0 <= n < 10_000:
        raise ValueError(f"number_words: {n} out of range")
    if n < 20:
        return _ONES[n]
    if n < 100:
        tens, ones = divmod(n, 10)
        return _TENS[tens] + (f"-{_ONES[ones]}" if ones else "")
    if n < 1000:
        hundreds, rest = divmod(n, 100)
        head = f"{_ONES[hundreds]} hundred"
        return head + (f" and {number_words(rest)}" if rest else "")
    thousands, rest = divmod(n, 1000)
    head = f"{_ONES[thousands]} thousand"
    if not rest:
        return head
    joiner = " and " if rest < 100 else " "
    return head + joiner + number_words(rest)


# ---------------------------------------------------------------------------
# status.md
# ---------------------------------------------------------------------------


def highest_phase(status_text: str) -> int:
    rows = [int(m.group(1)) for line in status_text.splitlines() if (m := ROW_RE.match(line))]
    if not rows:
        raise PendingError("docs/status.md has no phase rows")
    return max(rows)


def check_status_shape(status_text: str) -> None:
    """The CLAUDE.md rules, as assertions."""
    lines = status_text.splitlines()
    blanks = sum(1 for line in lines if line == "")
    if blanks != EXPECTED_BLANK_LINES:
        raise PendingError(
            f"docs/status.md has {blanks} blank lines, not {EXPECTED_BLANK_LINES} — the table is broken"
        )
    row_idx = [i for i, line in enumerate(lines) if ROW_RE.match(line)]
    if not row_idx:
        raise PendingError("docs/status.md has no phase rows")
    for a, b in zip(row_idx, row_idx[1:]):
        if b != a + 1:
            raise PendingError(f"docs/status.md: a non-row line sits between rows at lines {a + 1}–{b + 1}")
    numbers = [int(ROW_RE.match(lines[i]).group(1)) for i in row_idx]  # type: ignore[union-attr]
    if len(set(numbers)) != len(numbers):
        dupes = sorted({n for n in numbers if numbers.count(n) > 1})
        raise PendingError(f"docs/status.md: phase number(s) appear twice: {dupes}")
    # Strict ascending order is not asserted: row 116 was appended after 120 in
    # August 2026 and moving it is a separate, deliberate edit. What the stamp
    # relies on is that the LAST row carries the highest number.
    if numbers[-1] != max(numbers):
        raise PendingError(
            f"docs/status.md: the last row is phase {numbers[-1]} but the highest is {max(numbers)}"
        )
    # A citation on every row is the rule, but ~75 hand-written rows (346
    # among them) never carried one, so it is asserted only on rows the stamp
    # writes (apply_to_status), not on the file as found.
    intro = next((line for line in lines if INTRO_RE.match(line)), None)
    if intro is None:
        raise PendingError("docs/status.md: the intro sentence was not found")
    words = INTRO_RE.match(intro).group(2)  # type: ignore[union-attr]
    if words != number_words(numbers[-1]):
        raise PendingError(
            f"docs/status.md: intro says {words!r} phases but the highest row is {numbers[-1]} "
            f"({number_words(numbers[-1])!r})"
        )


def citation(commits: list[str]) -> str:
    if len(commits) == 1:
        return f"Commit `{commits[0]}`."
    return "Commits " + ", ".join(f"`{c}`" for c in commits) + "."


def _with_commas(n: int) -> str:
    return f"{n:,}"


def _strip_commas(s: str) -> int:
    return int(s.replace(",", ""))


def apply_to_status(status_text: str, pending: Pending, number: int, commits: list[str]) -> str:
    """Append one row for ``pending`` as phase ``number`` and fix the counts."""
    lines = status_text.splitlines()
    row_idx = [i for i, line in enumerate(lines) if ROW_RE.match(line)]
    if not row_idx:
        raise PendingError("docs/status.md has no phase rows")
    last = row_idx[-1]
    previous = int(ROW_RE.match(lines[last]).group(1))  # type: ignore[union-attr]
    if number != previous + 1:
        raise PendingError(f"stamp asked for phase {number} but the last row is {previous}")
    row = f"| {number} | **{pending.title}** {pending.body} {citation(commits)} |"
    assert CITATION_RE.search(ROW_RE.match(row).group(2))  # type: ignore[union-attr]
    lines.insert(last + 1, row)

    # Intro sentence: the spelled-out count follows the highest row.
    for i, line in enumerate(lines):
        m = INTRO_RE.match(line)
        if m:
            lines[i] = m.group(1) + number_words(number) + m.group(3) + line[m.end() :]
            break
    else:
        raise PendingError("docs/status.md: the intro sentence was not found")

    # Footer: apply the declared deltas; print before → after so the run shows it.
    if pending.tests:
        footer_idx = max(i for i, line in enumerate(lines) if line.startswith("Test suite:"))
        lines[footer_idx] = _apply_test_deltas(lines[footer_idx], pending.tests)

    out = "\n".join(lines) + "\n"
    check_status_shape(out)
    return out


def _apply_test_deltas(footer: str, deltas: dict[str, int]) -> str:
    if "backend" in deltas:
        m = BACKEND_COUNT_RE.search(footer)
        if not m:
            raise PendingError("footer: '<N> backend tests pass' not found")
        before = _strip_commas(m.group(1))
        after = before + deltas["backend"]
        print(f"  backend tests: {before:,} → {after:,}")
        footer = footer[: m.start(1)] + _with_commas(after) + footer[m.end(1) :]
    if "vitest" in deltas or "vitest_files" in deltas:
        m = VITEST_COUNT_RE.search(footer)
        if not m:
            raise PendingError("footer: '(<N> tests across <M> files' not found")
        tests_before = _strip_commas(m.group(1))
        files_before = int(m.group(2))
        tests_after = tests_before + deltas.get("vitest", 0)
        files_after = files_before + deltas.get("vitest_files", 0)
        print(f"  vitest: {tests_before:,} tests / {files_before} files → {tests_after:,} / {files_after}")
        replacement = f"({_with_commas(tests_after)} tests across {files_after} files"
        footer = footer[: m.start()] + replacement + footer[m.end() :]
    return footer


def apply_to_readme(readme_text: str, pending: Pending, number: int) -> str:
    new_line = f"**Latest: Phase {number}** — {pending.readme}"
    if not README_LATEST_RE.search(readme_text):
        raise PendingError("README.md: the '**Latest: Phase N** — …' line was not found")
    return README_LATEST_RE.sub(lambda _m: new_line, readme_text, count=1)


# ---------------------------------------------------------------------------
# git: which commit brought a pending file onto main, and in what order
# ---------------------------------------------------------------------------


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout.strip()


def landing_commit(path: Path) -> str:
    """Full sha of the first-parent commit that added ``path``.

    First-parent history of ``main`` is the sequence of merges (or squashes)
    that landed, so this is the merge commit of the pull request — the thing
    the changelog should link to. Falls back to the plain commit when the
    first-parent walk finds nothing (a file committed straight to main).
    """
    rel = str(path.relative_to(REPO))
    out = _git("log", "--first-parent", "--diff-filter=A", "--format=%H", "-n", "1", "--", rel)
    if not out:
        out = _git("log", "--diff-filter=A", "--format=%H", "-n", "1", "--", rel)
    if not out:
        raise PendingError(f"{path.name}: not in git history — commit it before stamping")
    return out


def ordered_by_landing(pending: list[Pending]) -> list[tuple[Pending, list[str]]]:
    """Pending files with the commits to cite, in the order they reached main.

    Ordered by position in first-parent history, not by timestamp: two merges
    within one second must still number in the order they landed.
    """
    history = _git("rev-list", "--first-parent", "HEAD").splitlines()
    position = {sha: i for i, sha in enumerate(history)}  # 0 = newest
    keyed = []
    for p in pending:
        sha = landing_commit(p.path)
        commits = p.commits or [_git("rev-parse", "--short=7", sha)]
        keyed.append((-position.get(sha, len(history)), p.slug, p, commits))
    keyed.sort(key=lambda t: (t[0], t[1]))
    return [(p, commits) for _, _, p, commits in keyed]


def changed_against(base: str) -> set[str]:
    out = _git("diff", "--name-only", f"{base}...HEAD")
    return {line for line in out.splitlines() if line}


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_check(base: str | None) -> int:
    try:
        pending = load_pending()
        status = STATUS_MD.read_text(encoding="utf-8")
        check_status_shape(status)
        if not README_LATEST_RE.search(README_MD.read_text(encoding="utf-8")):
            raise PendingError("README.md: the '**Latest: Phase N** — …' line was not found")
    except PendingError as exc:
        print(f"stamp-phase: {exc}", file=sys.stderr)
        return 1

    for p in pending:
        print(f"ok  {p.path.relative_to(REPO)}  —  {p.title}")
    if not pending:
        print("no pending changesets")

    if base:
        try:
            changed = changed_against(base)
        except subprocess.CalledProcessError as exc:
            print(f"stamp-phase: git diff against {base} failed: {exc.stderr.strip()}", file=sys.stderr)
            return 1
        touched = sorted(f for f in STAMPED_FILES if f in changed)
        if pending and touched:
            print(
                "stamp-phase: this branch adds a pending changeset AND edits "
                + ", ".join(touched)
                + " — the workflow stamps those on merge; revert the hand edits",
                file=sys.stderr,
            )
            return 1
        if touched and not pending:
            print(
                "note: this branch hand-stamps "
                + ", ".join(touched)
                + " with no pending changeset (the pre-347 way; still accepted)"
            )
    return 0


def cmd_apply(dry_run: bool) -> int:
    try:
        pending = load_pending()
        if not pending:
            print("no pending changesets — nothing to stamp")
            return 0
        status = STATUS_MD.read_text(encoding="utf-8")
        readme = README_MD.read_text(encoding="utf-8")
        check_status_shape(status)
        number = highest_phase(status)
        stamped: list[tuple[int, Pending]] = []
        for p, commits in ordered_by_landing(pending):
            number += 1
            print(f"Phase {number}: {p.title}  ({citation(commits)})")
            status = apply_to_status(status, p, number, commits)
            readme = apply_to_readme(readme, p, number)
            stamped.append((number, p))
    except (PendingError, subprocess.CalledProcessError) as exc:
        print(f"stamp-phase: {exc}", file=sys.stderr)
        return 1

    if dry_run:
        print("dry run — nothing written")
        return 0
    STATUS_MD.write_text(status, encoding="utf-8")
    README_MD.write_text(readme, encoding="utf-8")
    for _, p in stamped:
        p.path.unlink()
    # The last line is machine-readable: the workflow reads it for the commit message.
    numbers = ", ".join(str(n) for n, _ in stamped)
    print(f"STAMPED {numbers}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="validate pending files and the table")
    mode.add_argument("--apply", action="store_true", help="stamp pending files into status.md / README.md")
    ap.add_argument("--base", help="with --check: a ref to diff against (refuses hand edits + a pending file)")
    ap.add_argument("--dry-run", action="store_true", help="with --apply: print, write nothing")
    args = ap.parse_args(argv)
    if args.check:
        return cmd_check(args.base)
    return cmd_apply(args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
