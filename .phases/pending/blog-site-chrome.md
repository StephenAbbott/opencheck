---
title: The blog wears the app's header and footer.
readme: The blog's pages now use the same header, footer and logo as the rest of OpenCheck.
tests: backend +1
---
The server-rendered blog had its own chrome: the old triangle logo, a one-line navy footer and different spacing. Its header and footer now copy `SiteHeader` / `SiteFooter` (`frontend/src/components/SiteChrome.tsx`) as plain HTML: the magnifying-glass mark (`OpenCheckIcon`, copied coordinate for coordinate), the radial gradient, the same nav with Blog marked current, the GODIN ribbon, the brand and tagline, the Project and Legal link groups in the app's order and the licence strip, at the same breakpoints. The app's header search field is left out, because it starts a lookup only the app can run. The RSS link the old footer carried moves beside the blog's heading. `test_blog_chrome_matches_the_apps` parses SiteChrome.tsx and fails when the footer's Project links or the nav move in one place only. `TEMPLATE_VERSION` is 4.
