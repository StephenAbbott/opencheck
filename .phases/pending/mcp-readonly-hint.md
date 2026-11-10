---
title: Every OpenCheck MCP tool now declares MCP tool annotations, and only opencheck_save_report says it writes.
readme: OpenCheck's MCP tools now tell clients which calls only read, so read-only checks need no confirmation prompt.
tests: backend +3
---
Follow-up to the OpenSanctions MCP review (the readOnlyHint ticket). Fourteen tools declare `readOnlyHint: true`; a lookup spends the shared lookup budget and may fill the replay cache but changes nothing a caller can see, and `opencheck_discrepancy_note` drafts a note and sends nothing. `opencheck_save_report` stores a new report on every call, so it declares `readOnlyHint: false`, `destructiveHint: false` (additive) and `idempotentHint: false` (a new report id and manage token each time). `openWorldHint` is false only for `opencheck_knowability` and `opencheck_list_sources`, which read OpenCheck's own tables. The hints are advice to a client, never a security boundary. `test_mcp.py` pins the annotations on every tool and their serialised `tools/list` shape; `docs/mcp-server.md` says the same. After deploy, check `tools/list` from a client (Claude, the MCP Inspector) shows the annotations.
