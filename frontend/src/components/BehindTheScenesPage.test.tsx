/**
 * The about page links the data user's guide to OpenCheck's dates (Phase 318).
 *
 * The BODS dates guidance asks publishers to "create accompanying guidance for
 * data users"; `docs/dates.md` is that guidance, and until this phase nothing a
 * reader could reach from the site pointed at it.
 */

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { BehindTheScenesPage } from "./BehindTheScenesPage";

const DATES_DOC = "https://github.com/StephenAbbott/opencheck/blob/main/docs/dates.md";
const IDENTIFIERS_DOC = "https://github.com/StephenAbbott/opencheck/blob/main/docs/identifiers.md";

describe("BehindTheScenesPage", () => {
  it("links the dates guide from the BODS spine and from the reading list", () => {
    render(<BehindTheScenesPage />);
    const links = screen
      .getAllByRole("link")
      .filter((a) => a.getAttribute("href") === DATES_DOC);
    expect(links.length).toBe(2);
    for (const link of links) {
      expect(link.getAttribute("target")).toBe("_blank");
      expect(link.getAttribute("rel")).toContain("noreferrer");
    }
  });

  it("links the identifiers guide from the BODS spine and from the reading list", () => {
    // The identifiers counterpart to the dates guide (Phases 396–398): the
    // card used to say statement ids were "SHA-256 of source + type + local
    // key" and never changed, which has been the record id, not the
    // statement id, since Phase 398.
    render(<BehindTheScenesPage />);
    const links = screen
      .getAllByRole("link")
      .filter((a) => a.getAttribute("href") === IDENTIFIERS_DOC);
    expect(links.length).toBe(2);
    expect(screen.getByText(/a statement id derived from the record id/)).toBeTruthy();
    expect(screen.queryByText(/re-running a lookup always produces the same IDs/)).toBeNull();
    const guidance = screen.getByRole("link", { name: /BODS record identifiers guidance/ });
    expect(guidance.getAttribute("href")).toBe(
      "https://standard.openownership.org/en/0.4.0/standard/modelling/record-ids.html",
    );
  });

  it("names the four dates the guide explains", () => {
    render(<BehindTheScenesPage />);
    expect(
      screen.getByText(/when an interest was true, when the source declared it/),
    ).toBeTruthy();
  });

  it("links the standard's own dates guidance at the pinned 0.4.0 version", () => {
    render(<BehindTheScenesPage />);
    const guidance = screen.getByRole("link", { name: /BODS dates guidance/ });
    expect(guidance.getAttribute("href")).toBe(
      "https://standard.openownership.org/en/0.4.0/standard/modelling/dates-guidance.html",
    );
  });

  it("draws the lookup and keeps every step in words for a phone", () => {
    // The "Make the About page more visual" ticket: the diagram is hidden
    // under `sm`, so the numbered list must carry every step on its own.
    render(<BehindTheScenesPage />);
    expect(screen.getByRole("img", { name: /How a lookup works/ })).toBeTruthy();
    const steps = screen.getByRole("heading", { name: "How a lookup works" })
      .closest("section")!
      .querySelectorAll("ol > li");
    expect(steps.length).toBe(5);
    expect(screen.getByText("Aggregates risk signals")).toBeTruthy();
  });

  it("shows no third-party logo", () => {
    // GLEIF's and Neo4j's logos need a licence and Open Ownership's terms do
    // not cover theirs (checked 20 Nov 2026): every picture here is
    // OpenCheck's own (BOVS icons as data URIs, flags served by this site).
    const { container } = render(<BehindTheScenesPage />);
    const sources = [
      ...Array.from(container.querySelectorAll("img")).map((i) => i.getAttribute("src") ?? ""),
      ...Array.from(container.querySelectorAll("image")).map((i) => i.getAttribute("href") ?? ""),
    ];
    expect(sources.length).toBeGreaterThan(0);
    for (const src of sources) {
      expect(src.startsWith("data:image/svg+xml") || src.startsWith("/bods-dagre-images/flags/")).toBe(true);
    }
  });
});
