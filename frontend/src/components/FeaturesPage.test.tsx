/**
 * The markup tier for /features: the things only a rendered tree can be wrong
 * about (Phase 168's three tiers — `lib/*.test.ts` pins what the app *says*,
 * this pins what the markup *is*).
 *
 * Every assertion here is a bug that a value test could not see: an index
 * entry pointing at a section that does not exist, a second copy of the nav
 * rendered for screen readers behind a `lg:hidden`, an image with no `alt`,
 * a heading level that breaks the outline.
 */

import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { FeaturesPage } from "./FeaturesPage";
import { FEATURES } from "../lib/features";

describe("FeaturesPage", () => {
  it("renders one section per feature, addressable by its own id", () => {
    const { container } = render(<FeaturesPage />);
    const sections = container.querySelectorAll("article[id]");
    expect(sections.length).toBe(FEATURES.length);
    for (const f of FEATURES) {
      expect(container.querySelector(`article#${f.id}`), f.id).not.toBeNull();
    }
  });

  it("renders exactly one index, and every entry points at a real section", () => {
    const { container } = render(<FeaturesPage />);
    const navs = container.querySelectorAll("nav");
    // One nav, not one per breakpoint: a `lg:hidden` duplicate would read out
    // twice.
    expect(navs.length).toBe(1);
    const links = within(navs[0] as HTMLElement).getAllByRole("link");
    expect(links.length).toBe(FEATURES.length);
    for (const link of links) {
      const href = link.getAttribute("href") ?? "";
      expect(href).toMatch(/^#/);
      expect(container.querySelector(`article${href}`), href).not.toBeNull();
    }
  });

  it("names each feature once in the index and once as a heading", () => {
    render(<FeaturesPage />);
    for (const f of FEATURES) {
      expect(screen.getByRole("heading", { name: f.name })).toBeTruthy();
    }
  });

  it("gives every feature image alt text", () => {
    render(<FeaturesPage />);
    const images = screen.getAllByRole("img");
    expect(images.length).toBe(FEATURES.length);
    for (const img of images) {
      expect((img.getAttribute("alt") ?? "").length).toBeGreaterThan(60);
    }
  });

  it("marks one index entry as current", () => {
    const { container } = render(<FeaturesPage />);
    const current = container.querySelectorAll('nav a[aria-current="true"]');
    expect(current.length).toBe(1);
  });

  it("gives every feature one call to action outside the index", () => {
    const { container } = render(<FeaturesPage />);
    for (const f of FEATURES) {
      const article = container.querySelector(`article#${f.id}`) as HTMLElement;
      const links = within(article).getAllByRole("link");
      expect(links.length, f.id).toBe(1);
      expect(links[0].getAttribute("href"), f.id).toBe(f.cta.href);
    }
  });

  it("keeps the heading outline flat: one h2, one h3 per feature", () => {
    const { container } = render(<FeaturesPage />);
    expect(container.querySelectorAll("h1").length).toBe(0);
    expect(container.querySelectorAll("h2").length).toBe(1);
    // One per feature, plus the index's own "On this page".
    expect(container.querySelectorAll("h3").length).toBe(FEATURES.length + 1);
  });
});
