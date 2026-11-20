---
title: The blog drops its topic chips and filters.
readme: The blog no longer shows topic chips or a topic filter; posts are listed by date alone.
---
Stephen's call (20 Nov 2026): with twelve posts, filtering by topic is not needed yet. The index loses its filter row and each card its chips, and a post's "About this post" panel loses its Topics row. Topics stay in each post's frontmatter, the JSON-LD keywords, the article:tag meta and the RSS categories, so the filters can come back without re-importing. An old `/blog?topic=` link now shows every post. TEMPLATE_VERSION is 2 so cached pages re-fetch.
