/**
 * Parse the OpenCheck development-history table (docs/status.md) into changelog
 * entries. The page only shows an item if it earned a phase row in status.md —
 * that table is the editorial gate. Each entry surfaces the phase title and the
 * first sentence of the update, plus links to the commit(s) it cites.
 */

export interface ChangelogEntry {
  phase: number;
  title: string;
  summary: string;
  commits: string[];
}

export const REPO_URL = "https://github.com/StephenAbbott/opencheck";

export function commitUrl(hash: string): string {
  return `${REPO_URL}/commit/${hash}`;
}

const _ABBREVIATIONS = ["e.g.", "i.e.", "etc.", "vs.", "no.", "al.", "inc.", "ltd.", "plc."];

/** Strip inline markdown (links → text, bold, code, escaped pipes) for display. */
function stripMarkdown(s: string): string {
  return s
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1") // [text](url) → text
    .replace(/\*\*([^*]+)\*\*/g, "$1") // **bold** → bold
    .replace(/`([^`]+)`/g, "$1") // `code` → code
    .replace(/\\([|*_`])/g, "$1") // unescape \| \* etc.
    .replace(/\s+/g, " ")
    .trim();
}

/** The first sentence of a piece of prose, tolerant of decimals + abbreviations. */
function firstSentence(text: string): string {
  const re = /[.!?](\s|$)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const idx = m.index;
    // Skip a period that ends a known abbreviation (e.g., i.e., etc.).
    const lead = text.slice(Math.max(0, idx - 4), idx + 1).toLowerCase();
    if (_ABBREVIATIONS.some((a) => lead.endsWith(a))) continue;
    return text.slice(0, idx + 1).trim();
  }
  return text.trim();
}

/** Commit hashes from the trailing "Commit `x`." / "Commits `a`, `b`." clause. */
function extractCommits(headline: string): string[] {
  const at = headline.search(/Commits?\b/i);
  if (at < 0) return [];
  const tail = headline.slice(at);
  const hashes = [...tail.matchAll(/`([0-9a-f]{7,40})`/g)].map((m) => m[1]);
  return [...new Set(hashes)];
}

/** A status.md row looks like `| 64 | <headline> |`; the headline is one cell. */
function parseRow(line: string): ChangelogEntry | null {
  const trimmed = line.trim();
  if (!trimmed.startsWith("|")) return null;
  // Strip the outer pipes, then split on the FIRST inner pipe only (the phase
  // column never contains a pipe; the headline may).
  const inner = trimmed.replace(/^\|/, "").replace(/\|\s*$/, "");
  const sep = inner.indexOf("|");
  if (sep < 0) return null;
  const phaseRaw = inner.slice(0, sep).trim();
  const phase = Number.parseInt(phaseRaw, 10);
  if (!Number.isFinite(phase) || String(phase) !== phaseRaw) return null; // skip header/divider

  const headline = inner.slice(sep + 1).trim();
  const commits = extractCommits(headline);

  // Title = the lead clause before the first " — " em-dash; summary = the first
  // sentence after it. Rows without an em-dash use the first sentence as title.
  const dash = headline.indexOf(" — ");
  let title: string;
  let summary: string;
  if (dash >= 0) {
    title = stripMarkdown(headline.slice(0, dash));
    summary = stripMarkdown(firstSentence(headline.slice(dash + 3)));
  } else {
    title = stripMarkdown(firstSentence(headline));
    summary = "";
  }
  return { phase, title, summary, commits };
}

/** Parse the whole status.md into changelog entries, newest phase first. */
export function parseStatusMarkdown(md: string): ChangelogEntry[] {
  const entries: ChangelogEntry[] = [];
  for (const line of md.split("\n")) {
    const entry = parseRow(line);
    if (entry) entries.push(entry);
  }
  entries.sort((a, b) => b.phase - a.phase);
  return entries;
}
