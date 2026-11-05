/**
 * The MCP server as the /api page explains it.
 *
 * The page used to carry its own list of tools, typed into the component, and
 * it drifted: by 5 Nov 2026 it named 12 of the 15 tools the server lists, and
 * none of what the handshake instructions tell an agent (the order of calls,
 * how to read the answers, which calls spend the lookup budget). This file
 * follows the handshake (`_INSTRUCTIONS` in `backend/opencheck/mcp/server.py`)
 * step for step, and `backend/tests/test_api_page_mcp.py` parses it: every
 * name in `TOOL_NAMES` must appear here exactly once, and `spendsBudget` must
 * be true for exactly the tools the handshake's "Cost." paragraph names.
 */

export interface McpTool {
  /** The tool's name, exactly as the server lists it. */
  name: string;
  /** The main arguments, as an agent would pass them. Optional ones end "?". */
  args: string;
  summary: string;
  /** True when a call spends the shared per-address lookup budget. */
  spendsBudget: boolean;
}

export interface McpReadingRule {
  term: string;
  text: string;
}

export interface McpStep {
  id: string;
  title: string;
  lede: string;
  tools: McpTool[];
  rules?: McpReadingRule[];
  note?: { text: string; linkLabel: string; href: string };
}

export const MCP_BULK_DATASETS_URL = "https://github.com/StephenAbbott/opencheck/releases";

export const MCP_STEPS: McpStep[] = [
  {
    id: "find",
    title: "Find the company",
    lede: "Turn a name or a registration number into an LEI. Neither call spends the lookup budget.",
    tools: [
      {
        name: "opencheck_search",
        args: "query",
        summary:
          "Candidates for a name, each with its LEI, jurisdiction and register identifiers[]. Filters narrow by jurisdiction, legal form or status.",
        spendsBudget: false,
      },
      {
        name: "opencheck_resolve_national_id",
        args: "number, country",
        summary: "A company-registration number to its LEI or LEIs.",
        spendsBudget: false,
      },
    ],
  },
  {
    id: "expect",
    title: "Set expectations, cheaply",
    lede:
      "Before a lookup, learn what the country's registers can show. A thin answer then reads as the register's limit, not as a finding: Cyprus publishes officers, not beneficial owners.",
    tools: [
      {
        name: "opencheck_knowability",
        args: "jurisdictions",
        summary:
          "Per country: what its registers publish about owners, who may see it, and which OpenCheck sources read it. A country with no notes is answered as such, never with an error.",
        spendsBudget: false,
      },
      {
        name: "opencheck_list_sources",
        args: "",
        summary: "Every data source, with its licence and live status.",
        spendsBudget: false,
      },
    ],
  },
  {
    id: "check",
    title: "Check it",
    lede: "Start with a lookup, then ask the narrower questions you need.",
    tools: [
      {
        name: "opencheck_lookup",
        args: "lei",
        summary: "Identity, identifiers, owners, controllers, risk signals and source coverage.",
        spendsBudget: true,
      },
      {
        name: "opencheck_register_lookup",
        args: "scheme, id",
        summary:
          "The same for a company with no LEI (most UK LLPs, most small companies), by a search candidate's identifiers[]: for example GB-COH, OC346224.",
        spendsBudget: true,
      },
      {
        name: "opencheck_owners",
        args: "lei, threshold?",
        summary:
          "Who holds above a threshold (the national rule by default): owners calculated from shareholdings per source, the beneficial owners registers declare, and who is in both. Adds the sanctions 50% test for OFAC, the EU and the UK, ownership only.",
        spendsBudget: true,
      },
      {
        name: "opencheck_ownership_map",
        args: "lei",
        summary:
          "The ownership structure as an SVG image, every edge as a text row, and a link to the interactive FullCheck view.",
        spendsBudget: true,
      },
      {
        name: "opencheck_export_bods",
        args: "lei, format?",
        summary:
          "The full ownership-and-control graph as BODS v0.4 statements; include_subsidiaries folds in the GLEIF subsidiary network.",
        spendsBudget: true,
      },
      {
        name: "opencheck_subsidiaries",
        args: "lei",
        summary:
          "What the company consolidates: its GLEIF Level 2 network as counts, rows or BODS, optionally with the MEIP, EITI and GEM lists kept apart.",
        spendsBudget: false,
      },
      {
        name: "opencheck_person_check",
        args: "name, birth_year?",
        summary:
          "Screen one person for politically exposed, sanctions and offshore-leaks matches. Name-based, so every match is possible, never confirmed.",
        spendsBudget: false,
      },
      {
        name: "opencheck_person_positions",
        args: "scheme, id",
        summary:
          "One person's positions across companies, by a register's own person key (the Estonian register's pseudonymous key or a Companies House officer id). Never by name.",
        spendsBudget: false,
      },
    ],
  },
  {
    id: "read",
    title: "Read the answer honestly",
    lede: "Say what was read and what was not.",
    tools: [],
    rules: [
      {
        term: "degraded_sources",
        text: "names the sources that did not answer. A screen with any is incomplete, never clean.",
      },
      { term: "kind", text: "is risk or context on every signal. Context is never a finding." },
      {
        term: "Possible name match",
        text: "is evidence to weigh, not an identity.",
      },
      {
        term: "Integrated ownership",
        text: "figures are per source and are never added across sources.",
      },
      {
        term: "GLEIF Level 2",
        text: "is accounting consolidation, not ownership.",
      },
      {
        term: "license_notices",
        text: "travel with the answer. Pass them on: some sources are CC BY-NC.",
      },
    ],
  },
  {
    id: "keep",
    title: "Keep it or hand it on",
    lede: "Freeze what was found, or tell a register its record disagrees with another.",
    tools: [
      {
        name: "opencheck_save_report",
        args: "lei",
        summary:
          "Keeps a check as a link with a SHA-256 of exactly what was found. The reader can verify it; nothing is re-checked.",
        spendsBudget: true,
      },
      {
        name: "opencheck_discrepancy_note",
        args: "lei or scheme, id",
        summary:
          "Drafts the note to send a register when its record disagrees with another source's.",
        spendsBudget: true,
      },
    ],
  },
  {
    id: "many",
    title: "Many companies",
    lede: "For a short list, batch the lookups. To explore hundreds, start from the bulk data instead.",
    tools: [
      {
        name: "opencheck_batch_lookup",
        args: "leis",
        summary: "Up to 20 LEIs at once: one compact row each, with failed rows kept apart.",
        spendsBudget: true,
      },
    ],
    note: {
      text: "Looping over live lookups is slow and spends the budget. The Estonia and Azerbaijani Laundromat datasets are published as downloads.",
      linkLabel: "Bulk datasets on GitHub",
      href: MCP_BULK_DATASETS_URL,
    },
  },
];

export const MCP_TOOLS: McpTool[] = MCP_STEPS.flatMap((s) => s.tools);

export const MCP_BUDGET_TOOLS: string[] = MCP_TOOLS.filter((t) => t.spendsBudget).map((t) => t.name);
