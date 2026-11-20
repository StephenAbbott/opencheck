---
title: The blog's list shows each post's lead image beside its card.
readme: The blog's list of posts now shows each post's lead image beside its title, and the Nigeria post links Nestle Nigeria to its own LEI.
tests: backend +2
---
Each card on /blog carries the post's lead image (the `image` already in its frontmatter, the share card the importer took from Medium) as a 240px thumbnail on the right, above the text on a phone. The images are 1.9:1 share cards with text in them, so they are shown at that shape, never cropped to another; the image is decorative (`alt=""`, `aria-hidden`, out of the tab order) because the title beside it is the link, and it loads lazily with its width and height set so nothing shifts. `TEMPLATE_VERSION` is 3, so cached pages refresh. The Nigeria post's tenth example linked Nestle Nigeria PLC to MTN Nigeria's LEI; it now links to 0292002173F4KD5E5347 (Stephen, 20 Nov 2026).
