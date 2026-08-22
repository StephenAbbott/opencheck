import { describe, expect, it } from "vitest";
import { rowFinding } from "./sourceFinding";

describe("rowFinding", () => {
  it("leads with the sentence and keeps the fragment underneath", () => {
    expect(
      rowFinding({
        finding: "2 people with significant control on file.",
        summary: "GB · registered entity",
      }),
    ).toEqual({
      lead: "2 people with significant control on file.",
      sub: "GB · registered entity",
    });
  });

  it("falls back to the fragment for an adapter with no template yet", () => {
    // ~30 adapters have no finding template. Their rows must look exactly
    // like v1 — not empty, not broken — so the seven that do have one can
    // land one at a time.
    expect(rowFinding({ summary: "SG-UEN 12345 · live" })).toEqual({
      lead: "SG-UEN 12345 · live",
      sub: null,
    });
    expect(rowFinding({ summary: "SG-UEN 12345 · live", finding: null })).toEqual({
      lead: "SG-UEN 12345 · live",
      sub: null,
    });
  });

  it("never renders the same string twice", () => {
    const r = rowFinding({ finding: "Same text", summary: "Same text" });
    expect(r).toEqual({ lead: "Same text", sub: null });
  });

  it("treats whitespace-only values as absent", () => {
    expect(rowFinding({ finding: "   ", summary: "GB · registered entity" })).toEqual({
      lead: "GB · registered entity",
      sub: null,
    });
    expect(rowFinding({ finding: "A real finding.", summary: "  " })).toEqual({
      lead: "A real finding.",
      sub: null,
    });
  });

  it("renders nothing rather than an empty line when a source says neither", () => {
    expect(rowFinding({ summary: "" })).toBeNull();
    expect(rowFinding({ summary: "", finding: "" })).toBeNull();
  });
});
