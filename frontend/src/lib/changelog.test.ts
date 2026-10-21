import { describe, it, expect } from "vitest";
import { parseStatusMarkdown, commitUrl } from "./changelog";

const SAMPLE = `# OpenCheck — Development History

OpenCheck has shipped through sixty-four phases.

| Phase | Headline |
|------:|----------|
| 0 | Scaffold — FastAPI + React/Vite + 6 stub source adapters |
| 62 | Wikidata controlling-owner extraction + \`STATE_CONTROLLED\` signal — foundation / family / state ownership from Wikidata, mapped to BODS v0.4. **Origin:** a deeper investigation. Commits \`a2b4c79\`, \`d68506c\`, \`af9feef\`. |
| 64 | Subsidiary network in the main export (opt-in) — the network was a separate lazy view, e.g. behind a flag. Now opt-in. Commit \`e3c5f28\`. |

Test suite: 2160 backend tests.
`;

describe("parseStatusMarkdown", () => {
  const entries = parseStatusMarkdown(SAMPLE);

  it("extracts one entry per phase row, newest first, skipping header/divider", () => {
    expect(entries.map((e) => e.phase)).toEqual([64, 62, 0]);
  });

  it("splits title (before the em-dash) from the first-sentence summary", () => {
    const e = entries.find((x) => x.phase === 62)!;
    expect(e.title).toBe("Wikidata controlling-owner extraction + STATE_CONTROLLED signal");
    // first sentence only; decimals (v0.4) don't end the sentence prematurely;
    // markdown (** and backticks) is stripped; first letter capitalised.
    expect(e.summary).toBe(
      "Foundation / family / state ownership from Wikidata, mapped to BODS v0.4."
    );
  });

  it("does not cut the sentence on an abbreviation (e.g.)", () => {
    const e = entries.find((x) => x.phase === 64)!;
    expect(e.summary).toBe("The network was a separate lazy view, e.g. behind a flag.");
  });

  it("capitalises the first letter of every description", () => {
    for (const e of entries) {
      if (!e.summary) continue;
      const first = e.summary[0];
      expect(first).toBe(first.toUpperCase());
    }
  });

  it("extracts single and multiple commit hashes from the trailing clause", () => {
    expect(entries.find((x) => x.phase === 64)!.commits).toEqual(["e3c5f28"]);
    expect(entries.find((x) => x.phase === 62)!.commits).toEqual([
      "a2b4c79", "d68506c", "af9feef",
    ]);
  });

  it("handles a row with no commit reference", () => {
    const e = entries.find((x) => x.phase === 0)!;
    expect(e.title).toBe("Scaffold");
    expect(e.commits).toEqual([]);
  });

  it("builds GitHub commit URLs", () => {
    expect(commitUrl("e3c5f28")).toBe(
      "https://github.com/StephenAbbott/opencheck/commit/e3c5f28"
    );
  });
});

// The three shapes rows have been written in. Every card should come out the
// same: one sentence as the heading, the next sentence as the summary.
const SHAPES = `| Phase | Headline |
|------:|----------|
| 106 | A name is not an identification. Phase 105 fixed how the hit was *described*; this fixes whether it attached — the gate now asks for corroboration. More detail. Commit \`1111111\`. |
| 384 | **Crime and regulatory topics on the company's own record now raise signals, on an exact LEI match only.** From the Notion ticket "Enforcement topics". The match is exact — no fuzzy names. Commit \`2222222\`. |
| 385 | **Coverage honesty — three tickets from the check. A second sentence in bold.** Then the narrative. Commit \`3333333\`. |
| 386 | **A stamped headline with no narrative.** Commits \`4444444\`, \`5555555\`. |
`;

describe("parseStatusMarkdown — one sentence as the heading, whatever the row's shape", () => {
  const byPhase = new Map(parseStatusMarkdown(SHAPES).map((e) => [e.phase, e]));

  it("stamped row: the bold headline is the heading, even with an em-dash later in the narrative", () => {
    const e = byPhase.get(384)!;
    expect(e.title).toBe(
      "Crime and regulatory topics on the company's own record now raise signals, on an exact LEI match only."
    );
    expect(e.summary).toBe('From the Notion ticket "Enforcement topics".');
    expect(e.commits).toEqual(["2222222"]);
  });

  it("stamped row: a bold lead of two sentences still gives a one-sentence heading", () => {
    const e = byPhase.get(385)!;
    expect(e.title).toBe("Coverage honesty — three tickets from the check.");
    expect(e.summary).toBe("A second sentence in bold.");
  });

  it("stamped row with nothing after the headline has no summary and no citation text", () => {
    const e = byPhase.get(386)!;
    expect(e.title).toBe("A stamped headline with no narrative.");
    expect(e.summary).toBe("");
    expect(e.commits).toEqual(["4444444", "5555555"]);
  });

  it("prose row: the first sentence is the heading, not everything up to a later em-dash", () => {
    const e = byPhase.get(106)!;
    expect(e.title).toBe("A name is not an identification.");
    expect(e.summary).toBe(
      "Phase 105 fixed how the hit was described; this fixes whether it attached — the gate now asks for corroboration."
    );
  });

  it("never puts the commit citation in a heading or summary", () => {
    for (const e of byPhase.values()) {
      expect(e.title).not.toMatch(/Commits? /);
      expect(e.summary).not.toMatch(/Commits? /);
    }
  });
});

