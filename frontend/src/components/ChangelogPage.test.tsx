/**
 * The changelog page lists the latest merged changes, one card shape for all of
 * them, and links to the full history for the rest.
 *
 * Since the release-steward workflow (phases stamped on `main` at merge), every
 * pull request that merges becomes a phase row, so the intro must not claim the
 * list is curated, and every card must look the same whichever era its row was
 * written in: one sentence in bold as the heading, then a summary.
 */

import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ChangelogPage } from "./ChangelogPage";
import { CHANGELOG_SHOWN, STATUS_URL } from "../lib/changelog";
import statusData from "../lib/statusMarkdown.json";

describe("ChangelogPage", () => {
  it("says every change is listed, not only significant ones", () => {
    render(<ChangelogPage />);
    const intro = screen.getByText(/What's new in OpenCheck, newest first/);
    expect(intro.textContent).not.toMatch(/significant enough/);
    expect(intro.textContent).toMatch(/when it merges/);
  });

  it("gives every card exactly one heading, with no markdown or commit citation in it", () => {
    const { container } = render(<ChangelogPage />);
    const cards = container.querySelectorAll("article");
    expect(cards.length).toBe(CHANGELOG_SHOWN);
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

  it("shows only the latest phases, newest first, under the count and the update date", () => {
    const { container } = render(<ChangelogPage />);
    const phases = [...container.querySelectorAll("article")].map((a) =>
      Number(/Phase (\d+)/.exec(a.textContent ?? "")![1]),
    );
    expect(phases).toHaveLength(CHANGELOG_SHOWN);
    expect([...phases].sort((a, b) => b - a)).toEqual(phases);
    const facts = screen.getByTestId("changelog-facts").textContent ?? "";
    expect(facts).toContain(`${phases[0]} phases shipped`);
    expect(facts).toMatch(/last updated \d{1,2} [A-Z][a-z]+ \d{4}/);
    expect(facts).toContain(`showing the latest ${CHANGELOG_SHOWN}`);
    expect((statusData as { generatedAt?: string }).generatedAt).toBeTruthy();
  });

  it("links to the full development history on GitHub after the list", () => {
    render(<ChangelogPage />);
    const link = screen.getByRole("link", { name: /Read all \d+ phases in the full development history/ });
    expect(link.getAttribute("href")).toBe(STATUS_URL);
    expect(link.getAttribute("target")).toBe("_blank");
  });
});
