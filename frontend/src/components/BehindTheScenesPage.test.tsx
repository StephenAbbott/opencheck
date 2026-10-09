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
});
