# Curated example summaries (pre-baked)

Static, pre-generated AI summaries for the homepage's curated examples, one JSON
file per LEI (`<LEI>.json`). The frontend (`NarrativePanel`) fetches
`/curated-narratives/<lei>.json` on load and, if present, shows the summary
instantly with **no model call**. Live lookups have no file here and fall back to
the on-demand "Generate summary" button.

Each file is a full `NarrativeResponse` (summary + per-claim citations + the
evidence packet), so it renders identically to a freshly generated one.

## Regenerate

Whenever the prompt, model, or curated set changes:

```bash
cd backend
ANTHROPIC_API_KEY=sk-ant-... uv run python scripts/build_curated_narratives.py
```

Then commit the updated `*.json` files. They are bundled into the static site at
build time (Vite copies `public/` into `dist/`).
