import { describe, expect, it } from "vitest";

import {
  QUICKSTART_LANGUAGES,
  QUICKSTART_LEI,
  QUICKSTART_NOTES,
  QUICKSTART_STEPS,
  STREAM_INTRO,
  quickstartSnippet,
  streamSnippet,
} from "./apiQuickstart";

const BASE = "https://api.example.org";

describe("API quickstart", () => {
  it("walks search, lookup and export in that order", () => {
    expect(QUICKSTART_STEPS.map((s) => s.route)).toEqual(["/search", "/lookup", "/export"]);
  });

  it.each(QUICKSTART_LANGUAGES.map((l) => l.id))("%s calls the three routes on the base URL", (lang) => {
    const code = quickstartSnippet(lang, BASE);
    const at = (route: string) => code.indexOf(route);
    for (const step of QUICKSTART_STEPS) expect(at(step.route), step.route).toBeGreaterThan(-1);
    expect(at("/search")).toBeLessThan(at("/lookup"));
    expect(at("/lookup")).toBeLessThan(at("/export"));
    expect(code).toContain(BASE);
    expect(code).toContain("degraded_sources");
    expect(code).not.toContain("undefined");
    expect(QUICKSTART_NOTES[lang]).toBeTruthy();
  });

  it("waits out a 429 instead of retrying at once", () => {
    for (const lang of ["python", "javascript"] as const) {
      const code = quickstartSnippet(lang, BASE);
      expect(code).toContain("429");
      expect(code).toContain("Retry-After");
    }
  });

  it("curl names the example LEI; the others take it from the search", () => {
    expect(quickstartSnippet("curl", BASE)).toContain(QUICKSTART_LEI);
    expect(quickstartSnippet("python", BASE)).not.toContain(QUICKSTART_LEI);
    expect(quickstartSnippet("javascript", BASE)).not.toContain(QUICKSTART_LEI);
  });

  it("streams the search, never a check stream, which refuses scripts", () => {
    for (const { id } of QUICKSTART_LANGUAGES) {
      const code = streamSnippet(id, BASE);
      expect(code).toContain(`${BASE}/stream`);
      expect(code).not.toContain("lookup-stream");
    }
    expect(streamSnippet("javascript", BASE)).toContain("events.close()");
    expect(STREAM_INTRO).toContain("403");
  });
});
