---
title: The mypy ratchet gives the same count locally as in CI.
readme: The type-check ratchet now refuses to run without the optional ftm extra instead of reporting a stale baseline.
tests: backend +2
---
Found by the codebase review of 20 Nov 2026, after a branch reported the type-check limit "out of date" on main. `opencheck/matching.py` imports `rigour` optionally: with the `ftm` extra installed (as CI installs it) mypy read the fallback `StrictFormat = None` as assigning to a class (`misc`, 1 error); without it, as an unknown name (0 errors). The ratchet then told anyone running it without the extra that main's baseline was stale. The ignore now names both codes, so the module carries 0 errors either way and the baseline drops to 111, and `scripts/mypy_ratchet.py` refuses to run (exit 2) when `rigour` or `followthemoney` is missing, saying `uv sync --extra ftm`. `tests/test_mypy_ratchet.py` pins the refusal.
