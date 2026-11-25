/**
 * The /api page's reference facts: where the machine-readable reference is,
 * the sample response, the limits and the errors.
 *
 * Values, not markup, so the logic-only suite and the backend can pin them.
 * `backend/tests/test_api_reference.py` parses this file and fails when a
 * limit here disagrees with `config.py`, the replay window with
 * `lookup_replay.py`, a described response key is not a `LookupResponse`
 * field, or the sample file loses a key or gains a restricted source.
 */

/** Paths on the API host (prefixed with the base URL on the page). */
export const REFERENCE_LINKS: { label: string; path: string; description: string }[] = [
  {
    label: "Interactive reference",
    path: "/docs",
    description: "Every endpoint with its parameters, which you can call from the browser (Swagger UI).",
  },
  {
    label: "Reference to read",
    path: "/redoc",
    description: "The same reference laid out as one long page (ReDoc).",
  },
  {
    label: "OpenAPI specification",
    path: "/openapi.json",
    description: "The machine-readable description of the API, for code generators, Postman and agents.",
  },
  {
    label: "llms.txt",
    path: "/llms.txt",
    description: "A short plain-text guide to OpenCheck for language models and agents.",
  },
];

export const DATA_DICTIONARY_URL =
  "https://github.com/StephenAbbott/opencheck/blob/main/docs/data-dictionary.md";
export const DATASETS_URL =
  "https://github.com/StephenAbbott/opencheck/blob/main/docs/datasets.md";

/** The recorded `/lookup` answer served from the site (frontend/public). */
export const SAMPLE_LOOKUP = {
  path: "/api-samples/lookup-taqa-bratani.json",
  lei: "213800E11LI1SCETU492",
  name: "TAQA BRATANI LIMITED",
  recorded: "25 Nov 2026",
  /** Built by backend/scripts/build_api_sample.py. */
  builder: "backend/scripts/build_api_sample.py",
};

/** The top-level keys of a `/lookup` answer worth knowing first. */
export const RESPONSE_KEYS: [string, string][] = [
  ["verdict", "One sentence saying what the check found, built from a template, never by a model."],
  ["subject_profile", "What the registers say the company is: legal form, status, founding date, address, each with the sources that state it."],
  ["hits", "One row per source that answered, with the identifiers that source itself publishes."],
  ["bods", "The merged BODS v0.4 statements: the company, the people and companies around it, and the relationships between them."],
  ["risk_signals", "Every signal the run raised. Each has a kind: risk, or context, which is never a finding."],
  ["degraded_sources", "Checks that did not fully run. Any entry here means the screen is incomplete, not clean."],
  ["cross_source_links", "Identifiers two or more sources share, which is how a record is corroborated."],
  ["source_liveness", "How current each source's data is: live, cached or a dated snapshot, with its dates."],
  ["disclosure_status", "What each source says about the company's owners: declared, exempt, not provided and so on."],
  ["knowability", "What the company's country publishes about owners and who may see it."],
  ["integrated_ownership", "How much each party holds directly and through other companies, per source, never added across sources."],
  ["license_notices", "Licence wording a source asks you to carry with its data."],
];

/** The three per-address request tiers (`config.py` defaults). */
export const RATE_TIERS: { tier: string; perMinute: number; covers: string }[] = [
  {
    tier: "default",
    perMinute: 60,
    covers: "Most endpoints: /deepen, /lookup-source, /history, /securities, /subsidiaries, /knowability, /resolve-national-id. /sources and /health are not limited.",
  },
  {
    tier: "lookup",
    perMinute: 10,
    covers: "Endpoints that run a full check or fan out to every source: /lookup, /lookup-register, /search, /report, /export, /person-check and their streams.",
  },
  {
    tier: "heavy",
    perMinute: 3,
    covers: "Expensive work: /narrative, /export/pdf, /export/markdown, /export-network, /batch-stream, /batch-export, saving a report.",
  },
];

/** Fresh checks per minute per address, whichever route or tool starts them. */
export const LOOKUP_BUDGET_PER_MINUTE = 10;
/** A repeat of the same check inside this window is served from memory, free. */
export const REPLAY_WINDOW_MINUTES = 15;

export const RATE_LIMIT_HEADERS = ["X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "Retry-After"];

/** Status codes the API answers with, and what each means here. */
export const ERROR_CODES: { status: number; meaning: string }[] = [
  { status: 400, meaning: "The identifier is malformed: an LEI with the wrong shape or check digits, an unknown register scheme." },
  { status: 403, meaning: "/lookup-stream refuses automated clients; use /lookup instead." },
  { status: 404, meaning: "Nothing to answer: an unknown source, a register number the register does not hold, a saved report that does not exist." },
  { status: 409, meaning: "The run a request refers to is no longer held (saving a report after the 15-minute window): run the check again." },
  { status: 422, meaning: "A required parameter is missing or the wrong type. Here detail is a list naming each parameter." },
  { status: 429, meaning: "A rate limit or the lookup budget is spent. Wait the Retry-After seconds, then try again; never retry at once." },
  { status: 502, meaning: "An upstream source did not answer a request that depends on it alone. The detail names the source." },
  { status: 503, meaning: "OpenCheck is running as many checks as it can at once (Retry-After 30), or a feature is not set up on this server." },
];

/** The body every error carries. */
export const ERROR_SHAPE = '{"detail": "\'BAD\' is not a valid LEI. ISO 17442 LEIs are 20-character alphanumeric strings (e.g. 213800LH1BZH3DI6G760)."}';
