---
title: The /sources page title matches /api and /features.
readme: The sources page now opens with the same large page title as the API and features pages.
---
The /sources page opened with a small eyebrow label (`SectionLabel`) where /api and /features open with a large Bitter heading. It now uses the same `h2` styling as those two pages (`font-head`, `clamp(1.35rem,3vw,1.8rem)`, `text-oo-ink`), labelled by `sources-heading`. The heading is still an `h2` reading "Sources", so the e2e smoke and chunk-failure checks are unchanged. Stephen asked for it on 20 Nov 2026.
