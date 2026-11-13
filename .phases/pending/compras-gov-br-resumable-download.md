---
title: The Compras.gov.br index build resumes its ~1 GB download instead of failing when the registry's CDN drops the connection.
readme: The Compras.gov.br index build now resumes an interrupted download of the Open Contracting Partnership file.
---
The first run of refresh-compras-gov-br-index.yml (13 Nov 2026) failed after 40 MB of the 1,009,490,875-byte full.jsonl.gz: the registry's CDN closed the connection mid-body. build_compras_gov_br_index.py now retries up to twenty times, resuming each attempt with a Range request pinned to the file's ETag by If-Range (a changed file restarts from zero), and refuses a file that is not exactly the announced size. Re-run the workflow by hand after merge.
