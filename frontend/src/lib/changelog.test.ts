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
