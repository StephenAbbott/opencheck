/**
 * The /api page's quickstart: find a company, check it, download its graph,
 * and read a stream, in curl, Python and JavaScript.
 *
 * Values, not markup, so `apiQuickstart.test.ts` can pin what the snippets
 * claim (the routes, the fields they read, the 429 handling) and the page
 * renders them as written. Every snippet was run against production when it
 * was written (25 Nov 2026).
 */

export type QuickstartLanguage = "curl" | "python" | "javascript";

export const QUICKSTART_LANGUAGES: { id: QuickstartLanguage; label: string }[] = [
  { id: "curl", label: "curl" },
  { id: "python", label: "Python" },
  { id: "javascript", label: "JavaScript" },
];

/** The example company: the one the sample answer is recorded for. */
export const QUICKSTART_NAME = "TAQA Bratani";
export const QUICKSTART_LEI = "213800E11LI1SCETU492";

/** What the three steps do, in order. */
export const QUICKSTART_STEPS = [
  { route: "/search", text: "Find the company by name and take its LEI from the GLEIF result." },
  { route: "/lookup", text: "Check it. Read degraded_sources first: if it lists anything, the screen is incomplete." },
  { route: "/export", text: "Download the ownership graph as BODS v0.4 JSON." },
] as const;

/** A note under each language's snippet. */
export const QUICKSTART_NOTES: Record<QuickstartLanguage, string> = {
  curl: "Uses jq to pick out the fields. A fresh check takes up to a minute; a repeat within 15 minutes answers at once.",
  python: "Needs the requests package. The helper waits out a 429 once, for the seconds Retry-After gives, rather than retrying at once.",
  javascript: "Runs in Node 18 or later, or in a browser. A browser cannot read Retry-After from another site, so the helper waits a minute.",
};

export function quickstartSnippet(lang: QuickstartLanguage, base: string): string {
  const q = encodeURIComponent(QUICKSTART_NAME);
  switch (lang) {
    case "curl":
      return [
        "# 1. Find the LEI",
        `curl -s "${base}/search?q=${q}" \\`,
        `  | jq '.hits[] | select(.source_id == "gleif") | {name, lei: .identifiers.lei}'`,
        "",
        "# 2. Check the company",
        `curl -s "${base}/lookup?lei=${QUICKSTART_LEI}" \\`,
        "  | jq '{verdict, incomplete: ([.degraded_sources[].source_id] | unique),",
        "        signals: [.risk_signals[] | {kind, code, summary}]}'",
        "",
        "# 3. Download the ownership graph as BODS",
        `curl -s -o taqa-bratani.bods.json "${base}/export?lei=${QUICKSTART_LEI}&format=json"`,
      ].join("\n");
    case "python":
      return [
        "import time",
        "import requests",
        "",
        `BASE = "${base}"`,
        "",
        "def get(path, **params):",
        "    r = requests.get(BASE + path, params=params, timeout=180)",
        "    if r.status_code == 429:  # rate limit or lookup budget spent",
        '        time.sleep(int(r.headers.get("Retry-After", "60")))',
        "        r = requests.get(BASE + path, params=params, timeout=180)",
        "    r.raise_for_status()",
        "    return r.json()",
        "",
        "# 1. Find the LEI",
        `hits = get("/search", q="${QUICKSTART_NAME}")["hits"]`,
        'lei = next(h["identifiers"]["lei"] for h in hits if h["source_id"] == "gleif")',
        "",
        "# 2. Check the company",
        'check = get("/lookup", lei=lei)',
        'print(check["verdict"])',
        'if check["degraded_sources"]:',
        '    print("Incomplete:", sorted({d["source_id"] for d in check["degraded_sources"]}))',
        'for signal in check["risk_signals"]:',
        '    print(signal["kind"], signal["code"], signal["summary"])',
        "",
        "# 3. Download the ownership graph as BODS",
        'bods = get("/export", lei=lei, format="json")',
        'print(len(bods), "statements")',
      ].join("\n");
    case "javascript":
      return [
        `const BASE = "${base}";`,
        "",
        "async function get(path, params) {",
        "  const url = `${BASE}${path}?${new URLSearchParams(params)}`;",
        "  let res = await fetch(url);",
        "  if (res.status === 429) { // rate limit or lookup budget spent",
        '    const wait = Number(res.headers.get("Retry-After") ?? 60);',
        "    await new Promise((done) => setTimeout(done, wait * 1000));",
        "    res = await fetch(url);",
        "  }",
        "  if (!res.ok) throw new Error(JSON.stringify((await res.json()).detail));",
        "  return res.json();",
        "}",
        "",
        "// 1. Find the LEI",
        `const { hits } = await get("/search", { q: "${QUICKSTART_NAME}" });`,
        'const lei = hits.find((h) => h.source_id === "gleif").identifiers.lei;',
        "",
        "// 2. Check the company",
        'const check = await get("/lookup", { lei });',
        "console.log(check.verdict);",
        "if (check.degraded_sources.length) {",
        '  console.log("Incomplete:", [...new Set(check.degraded_sources.map((d) => d.source_id))]);',
        "}",
        "for (const s of check.risk_signals) console.log(s.kind, s.code, s.summary);",
        "",
        "// 3. Download the ownership graph as BODS",
        'const bods = await get("/export", { lei, format: "json" });',
        'console.log(bods.length, "statements");',
      ].join("\n");
  }
}

/** Reading a stream: the name search, which sends each result as it arrives. */
export const STREAM_INTRO =
  "/stream sends a name search as server-sent events: a filters event, then source_started, " +
  "hit, source_error and source_completed for each source, then done. The check streams " +
  "(/lookup-stream and the others) are for the website and refuse scripts with a 403: a " +
  "script calls /lookup, which answers once the check is done.";

export function streamSnippet(lang: QuickstartLanguage, base: string): string {
  const q = encodeURIComponent(QUICKSTART_NAME);
  switch (lang) {
    case "curl":
      return `curl -sN "${base}/stream?q=${q}"`;
    case "python":
      return [
        "import json",
        "import requests",
        "",
        `url = "${base}/stream"`,
        `with requests.get(url, params={"q": "${QUICKSTART_NAME}"}, stream=True, timeout=120) as r:`,
        "    r.raise_for_status()",
        "    event = None",
        "    for line in r.iter_lines(decode_unicode=True):",
        '        if line.startswith("event:"):',
        "            event = line[6:].strip()",
        '        elif line.startswith("data:") and event == "hit":',
        "            hit = json.loads(line[5:])",
        '            print(hit["source_id"], hit["name"])',
        '        elif event == "done":',
        "            break",
      ].join("\n");
    case "javascript":
      return [
        "// In a browser. EventSource reconnects when a stream ends, so close it on done.",
        `const events = new EventSource("${base}/stream?q=${q}");`,
        'events.addEventListener("hit", (e) => {',
        "  const hit = JSON.parse(e.data);",
        "  console.log(hit.source_id, hit.name);",
        "});",
        'events.addEventListener("done", () => events.close());',
      ].join("\n");
  }
}
