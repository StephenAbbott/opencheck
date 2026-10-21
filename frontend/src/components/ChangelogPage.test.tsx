/**
 * The changelog page lists every merged change, one card shape for all of them.
 *
 * Since the release-steward workflow (phases stamped on `main` at merge), every
 * pull request that merges becomes a phase row, so the intro must not claim the
 * list is curated, and every card must look the same whichever era its row was
 * written in: one sentence in bold as the heading, then a summary.
 */

import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ChangelogPage } from "./ChangelogPage";

describe("ChangelogPage", () => {
  it("says every change is listed, not only significant ones", () => {
    render(<ChangelogPage />);
    const intro = screen.getByText(/Every change to OpenCheck, newest first/);
    expect(intro.textContent).not.toMatch(/significant enough/);
    expect(intro.textContent).toMatch(/when it merges/);
  });

  it("gives every card exactly one heading, with no markdown or commit citation in it", () => {
    const { container } = render(<ChangelogPage />);
    const cards = container.querySelectorAll("article");
    expect(cards.length).toBeGreaterThan(300);
    for (const card of cards) {
      const headings = within(card as HTMLElement).getAllByRole("heading", { level: 3 });
      expect(headings).toHaveLength(1);
      const text = headings[0].textContent ?? "";
      expect(text.length).toBeGreaterThan(0);
      expect(text).not.toMatch(/\*\*|`/);
      expect(text).not.toMatch(/Commits? [0-9a-f]{7}/);
    }
  });

  it("marks only the newest entry as Latest", () => {
    const { container } = render(<ChangelogPage />);
    const latest = screen.getAllByText("Latest");
    expect(latest).toHaveLength(1);
    expect(container.querySelector("article")!.contains(latest[0])).toBe(true);
  });
});
