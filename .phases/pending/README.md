# Pending changesets

A branch that ships a phase adds **one file here** and does **not** edit
`docs/status.md`, `README.md` or `frontend/src/lib/statusMarkdown.json`.
When the branch merges, `.github/workflows/stamp-phase.yml` runs
`backend/scripts/stamp_phase.py`, which assigns the next phase number, writes
the status row (citing the merge commit), bumps the spelled-out count, applies
the test-count deltas, rewrites the README's `Latest` line, regenerates the
changelog JSON, deletes this file and commits — all on `main`, after the fact.

Nothing on a branch names a phase number any more, so two branches in flight
cannot claim the same one, and nothing on a branch edits the shared files, so
they cannot conflict.

## File name

`<slug>.md` — a short kebab-case name for the work, ideally the branch name
without its prefix (`sanctions-50-rule.md`). Never a number. This README is
ignored.

## Format

```markdown
---
title: The bold headline, one sentence, ending in a full stop.
readme: One plain-prose sentence for the README's Latest line — no hash, no PR number.
tests: backend +12, vitest +3, vitest_files +1
---
The narrative that follows the headline in the status row. Write it as one or
more paragraphs; the stamp folds it into the single line a row has to be. Say
what changed, why, what was decided and what to check in production — the row
is the record future sessions read. Do not write the commit citation; the
stamp appends it.
```

- `title` and `readme` are required. `title` becomes `**title**` at the start
  of the row.
- `tests` is optional: deltas against the base you branched from, measured by
  running the suites with and without your change (`uv sync --extra ftm`, then
  `uv run pytest -q`; `npm test` for vitest). Keys: `backend`, `vitest`,
  `vitest_files`. Omit a key that did not change; omit the line when nothing
  did.
- `commits` is optional: explicit short hashes to cite instead of the merge
  commit (`commits: 3f1a2b4, 9c8d7e6`). Rarely needed.
- A literal `|` in the narrative is escaped for you.

## Checks

`python3 backend/scripts/stamp_phase.py --check` validates every file here;
the workflow runs it on each pull request that touches this directory or the
stamped files, and refuses a branch that both adds a changeset and hand-edits
the stamped files. `--apply --dry-run` shows the row that would be written.
