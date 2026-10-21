/**
 * Parse the OpenCheck development-history table (docs/status.md) into changelog
 * entries. Every merged phase has a row there — since Phase 347 the stamp
 * workflow writes it on `main` when a pull request merges — so the changelog is
 * a complete record of what has shipped, newest first.
 *
 * Rows have been written in three shapes over the project's life:
 *
 *   - **stamped** (Phase 134 on, and every row the stamp writes):
 *     `**One-sentence headline.** Narrative… Commit \`abc1234\`.`
 *   - **prose** (Phases 106–133): `Headline sentence. Narrative… Commit \`…\`.`
 *   - **legacy** (Phases 0–105): `Short title — narrative… Commit \`…\`.`
 *
 * Each becomes the same card: one sentence as the heading, the sentence after
 * it as the summary, and the commit(s) the row cites.
 */

export interface ChangelogEntry {
  phase: number;
  /** One sentence (or, on a legacy row, the short title before the em-dash). */
  title: string;
  /** The sentence after the title; empty when the row has nothing after it. */
  summary: string;
  commits: string[];
}

export const REPO_URL = "https://github.com/StephenAbbott/opencheck";

export function commitUrl(hash: string): string {
  return `${REPO_URL}/commit/${hash}`;
}

const _ABBREVIATIONS = ["e.g.", "i.e.", "etc.", "vs.", "no.", "al.", "inc.", "ltd.", "plc."];

/** Strip inline markdown (links → text, bold, italics, code, escaped pipes). */
function stripMarkdown(s: string): string {
  return s
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1") // [text](url) → text
    .replace(/\*\*([^*]+)\*\*/g, "$1") // **bold** → bold
    .replace(/(^|[^*\w])\*([^*\s][^*]*)\*(?![*\w])/g, "$1$2") // *italic* → italic
    .replace(/`([^`]+)`/g, "$1") // `code` → code
    .replace(/\\([|*_`])/g, "$1") // unescape \| \* etc.
    .replace(/\s+/g, " ")
    .trim();
}

/** Index just past the end of the first sentence, tolerant of decimals and
 * abbreviations; the text's length when it has no sentence end. */
function firstSentenceEnd(text: string): number {
  const re = /[.!?](\s|$)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const idx = m.index;
    // Skip a period that ends a known abbreviation (e.g., i.e., etc.).
    const lead = text.slice(Math.max(0, idx - 4), idx + 1).toLowerCase();
    if (_ABBREVIATIONS.some((a) => lead.endsWith(a))) continue;
    return idx + 1;
  }
  return text.length;
}

function firstSentence(text: string): string {
  return text.slice(0, firstSentenceEnd(text)).trim();
}

/** Capitalise the first character so each summary reads as a sentence. Legacy
 * summaries are continuations after the row's em-dash, so they usually start
 * lowercase ("a lazy reveal…"). A no-op when the first char isn't a letter. */
function capitaliseFirst(s: string): string {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

/** The trailing `Commit \`x\`.` / `Commits \`a\`, \`b\`.` clause every row ends with. */
const CITATION_RE = /\s*Commits?\s+`[0-9a-f]{7,40}`(?:\s*(?:,|and)\s*`[0-9a-f]{7,40}`)*[^`|]*$/i;

/** Commit hashes from the trailing citation clause. */
function extractCommits(headline: string): string[] {
  const at = headline.search(/Commits?\b/i);
  if (at < 0) return [];
  const tail = headline.slice(at);
  const hashes = [...tail.matchAll(/`([0-9a-f]{7,40})`/g)].map((m) => m[1]);
  return [...new Set(hashes)];
}

/** Split a row's text into its heading sentence and everything after it. */
function splitHeading(text: string): { title: string; rest: string } {
  // Stamped rows: the bold lead is the headline, one sentence by contract
  // (.phases/pending/README.md). Take its first sentence anyway, so a bold
  // lead that runs to two sentences still gives a one-sentence heading.
  const bold = /^\*\*(.+?)\*\*/.exec(text);
  if (bold) {
    const lead = bold[1];
    const end = firstSentenceEnd(lead);
    return {
      title: stripMarkdown(lead.slice(0, end)),
      rest: (lead.slice(end) + text.slice(bold[0].length)).trim(),
    };
  }
  const end = firstSentenceEnd(text);
  // Legacy rows: a short title, an em-dash, then the narrative. When the
  // em-dash falls inside the first sentence the title before it is the
  // headline; otherwise the em-dash is just punctuation in the prose.
  const dash = text.indexOf(" — ");
  if (dash >= 0 && dash < end) {
    return { title: stripMarkdown(text.slice(0, dash)), rest: text.slice(dash + 3).trim() };
  }
  return { title: stripMarkdown(text.slice(0, end)), rest: text.slice(end).trim() };
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
  const body = headline.replace(CITATION_RE, "").trim();

  const { title, rest } = splitHeading(body);
  const summary = capitaliseFirst(firstSentence(stripMarkdown(rest)));
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
