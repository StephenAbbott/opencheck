---
title: A blog post with a code block fits a phone screen.
readme: The blog's post pages no longer scroll sideways on a phone when a post carries a code block.
---
"What changed today" was 541px wide on a 390px iPhone screen: its curl command sits in a `<pre>`, and below 860px the post folds into a one-column grid declared `1fr`, whose minimum is its widest unbreakable content, so the title, the date box and the lead image all ran off the right edge with it. The column is now `minmax(0,1fr)`, as the two-column layout above 860px already was, and `.prose pre` gets its own style (soft box, horizontal scroll inside the block, no inline-code padding), which the blog had never had. `TEMPLATE_VERSION` 6 so the ETag changes. A Playwright test at 390px reads every post from the backend's blog sitemap plus the index and fails on any page wider than the screen; it fails on main and passes here. Check in production: the post on a phone, with the curl line scrolling inside its box.
