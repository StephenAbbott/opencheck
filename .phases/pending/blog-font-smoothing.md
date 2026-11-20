---
title: The blog's text is smoothed like the app's.
readme: The blog's wordmark and text no longer look bolder than the rest of OpenCheck on a Mac.
tests: backend +1
---
The app's body is Tailwind `antialiased` (`-webkit-font-smoothing: antialiased`, `-moz-osx-font-smoothing: grayscale`); the server-rendered blog set neither, so macOS drew its wordmark and every word heavier, though font, weight and size were identical (measured on production, 20 Nov 2026: Bitter 700 20px, 108.36px wide on both). The blog's body now sets both, and `test_blog_text_is_smoothed_like_the_apps` reads `frontend/index.html` so the two cannot part again. `TEMPLATE_VERSION` is bumped so cached pages refresh.
