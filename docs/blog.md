# The OpenCheck blog

`opencheck.world/blog` carries the posts about OpenCheck that Stephen first
published on Medium. opencheck.world is the original copy: every post page
names its own URL as `rel=canonical`, and the Medium story is credited as
where the post first appeared.

## Where things are

| What | Where |
|---|---|
| Posts | `data/blog/posts/<slug>.md` — Markdown with YAML frontmatter |
| Images | `data/blog/media/<slug>/NN.webp` |
| Renderer | `backend/opencheck/blog.py` (markdown-it-py, CommonMark, raw HTML escaped) |
| Routes | `backend/opencheck/routers/blog.py` |
| Importer | `backend/scripts/import_medium.py` |

Routes: `/blog` (every post, newest first, grouped by month), `/blog/<slug>`, `/blog/feed.xml` (RSS 2.0, full
text), `/blog/sitemap.xml` (named in `robots.txt`) and
`/blog/media/<slug>/<file>`.

## Frontmatter

```yaml
---
title: "Where did this come from, and is it still true?"
subtitle: "Shown under the title and used as the description"
published: "2026-11-02T15:51:59.475Z"   # required; the date every page shows
updated: "2026-12-01"                    # optional; shown and used for lastmod
medium_url: "https://medium.com/@stephenabbottpugh/…"
medium_id: "ff3c1f53535b"
topics:
  - "Beneficial Ownership"
image: "/blog/media/<slug>/01.webp"      # the hero image
image_alt: ""
---
```

`topics` are kept in the frontmatter, the JSON-LD `keywords`, the
`article:tag` meta and the feed's `<category>` lines, but no page shows them:
the blog has no topic chips or filters (Stephen, 20 Nov 2026).

The file name is the slug. Body headings start at `##` (the page's `h1` is the
title); each `##` joins the post's Contents list. A paragraph holding only an
image is drawn as a figure; the image's Markdown title becomes its caption.

## New posts arrive by themselves

Once a day (06:17 UTC, and on demand from the Actions tab), the
`import-medium-feed` workflow reads Medium's author RSS feed and imports every
post tagged `opencheck` that `data/blog/posts/` does not already hold, with
its images. It runs the blog tests on the new files and commits them to
`main`, which Render deploys. So a post published on Medium with the
OpenCheck tag is on opencheck.world/blog within about a day.

The feed is the posts' text as Medium sends it to feed readers, which differs
from the export in small ways: inline `code` spans arrive as plain text, the
published time has no milliseconds, and the subtitle is the italic first
paragraph (or, for a post that opens with a heading, its first paragraph, as
Medium shows it). A post is matched on its Medium id and **never rewritten**,
so a correction made here is kept; to pick up an edit made later on Medium,
delete the file and run the workflow, or re-import from the export.

The feed holds the latest ten posts only, so a post that has dropped out of
it (or one tagged after ten newer posts) needs the export below.

## Importing from the Medium export

The author feed holds only the latest ten posts and Medium's story pages
answer 403 to scripts, so the full archive comes from Medium's "Download your
information" zip (Settings → Security and apps → Download your information):

```
cd backend
uv run --with markdownify --with beautifulsoup4 \
    python scripts/import_medium.py ../blog/medium-export.zip
```

It imports every post tagged `opencheck` on Medium (add `--id <medium id>` for
one that is not), downloads its images as WebP, turns Medium's link cards into
plain links and points links between imported posts at `/blog/<slug>`.
Re-running replaces the files; `--keep-existing` leaves posts already here
alone, which is what to use once a post has been edited in this repository.
`--feed` (in place of the zip) is what the daily workflow runs.

The export zip holds personal data. Keep it in the repository's `blog/`
folder, which is ignored, and never commit it.

## After publishing a post

In Medium's story settings ("More settings" → "Customize canonical link"),
set the canonical link to the post's opencheck.world URL, so search engines
treat this copy as the original.
