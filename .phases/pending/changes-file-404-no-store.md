---
title: A refused daily changes file is never kept by Render's edge.
readme: The daily changes file addresses answer a missing file with no-store, so the edge cannot keep a 404 after the file exists.
tests: backend +1
---
Found in the post-deploy check of the /changes page (27 Nov 2026). `GET /changes/latest.jsonl.gz` answered 404 (no file had been built yet: the first is written by the first GLEIF delta after the deploy), and the 404 came back from Render's edge with `cf-cache-status: HIT`. The path ends in a static extension and the refusal carried no `Cache-Control`, so the edge kept it, and it could have hidden the day's file once it existed. Every refusal from `/changes/{name}` (an unknown name, the feature off, no file held, no file for the day) now carries `Cache-Control: no-store`, like the 302 and today's file already did. After deploy, check that `latest.jsonl.gz` answers 302 once the first GLEIF delta of the day has been applied.
