/**
 * The markup tier for the homepage "New features" band (Phase 344): what only
 * a rendered tree can be wrong about — the heading outline, one link per card
 * with a real name and href, and an image with its alt and intrinsic size.
 */

import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { NewFeatures } from "./HomePanels";
import { NEW_FEATURES } from "../lib/newFeatures";

describe("NewFeatures", () => {
  it("is one section labelled 'New features', with one h3 per feature in order", () => {
    render(<NewFeatures />);
    const heading = screen.getByRole("heading", { level: 2, name: "New features" });
    const section = heading.closest("section") as HTMLElement;
    expect(section).not.toBeNull();
    const titles = within(section)
      .getAllByRole("heading", { level: 3 })
      .map((h) => h.textContent);
    expect(titles).toEqual(NEW_FEATURES.map((f) => f.title));
  });

  it("gives each card exactly one link, named by its CTA and pointing at its page", () => {
    const { container } = render(<NewFeatures />);
    const cards = container.querySelectorAll("article");
    expect(cards.length).toBe(NEW_FEATURES.length);
    NEW_FEATURES.forEach((f, i) => {
      const links = within(cards[i] as HTMLElement).getAllByRole("link");
      expect(links.length, f.id).toBe(1);
      expect(links[0]).toHaveAccessibleName(f.cta);
      expect(links[0].getAttribute("href")).toBe(f.href);
    });
  });

  it("draws each illustration with its alt text and intrinsic size", () => {
    render(<NewFeatures />);
    for (const f of NEW_FEATURES) {
      const img = screen.getByAltText(f.imageAlt) as HTMLImageElement;
      expect(img.getAttribute("src")).toBe(f.image);
      expect(img.getAttribute("width")).toBe("320");
      expect(img.getAttribute("height")).toBe("168");
    }
  });

  it("renders no button — a promo card links to a page, it does not run a lookup", () => {
    render(<NewFeatures />);
    expect(screen.queryAllByRole("button")).toHaveLength(0);
  });
});
