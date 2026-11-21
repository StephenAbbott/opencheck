---
title: The blog's own header now hides its Blog link on phones, as every other page's header does.
readme: On phones the blog's header no longer shows a Blog link, matching the rest of the site; the footer still links to it.
tests: backend +1
---
`SiteHeader` hides its Blog link below `sm` (640px, `hidden sm:inline-flex`) because a fourth word does not fit the one-row phone header; the server-rendered blog header copied the link without that rule, so `/blog` and its posts showed Blog on phones while the homepage and /features did not (Stephen, 21 Nov 2026). The link now carries `nav-blog`, `display:none` below 640px and `inline-flex` above, in the blog stylesheet's existing 640px breakpoint. The footer link is unchanged at every width. `test_blog_link_is_desktop_only_in_the_header` pins the rule on both sides (it reads `SiteChrome.tsx`) and that the footer keeps the link; checked in Chromium at 390px (Blog `display: none`) and 1280px (shown).
