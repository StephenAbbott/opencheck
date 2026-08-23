/** @type {import('tailwindcss').Config} */
//
// Beneficial Ownership design-system tokens, derived from the
// "BOVS Design Library" handoff (CC BY 4.0, Open Ownership 2020 brand).
//
// Colour names map 1:1 to the design system's CSS variables so the
// migration from raw `#hex` values to Tailwind utilities is a sed
// away.
//
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        oo: {
          navy: "#191d23",   // header bg, body text, card titles
          burst: "#363f4e",  // icon strip bg, link hover
          blue: "#3d30d4",   // accents, indices, links
          light: "#dceeff",  // image placeholder, header eyebrow
          green: "#25cb55",  // available
          ink: "#191d23",    // body text alias
          muted: "#696969",  // secondary copy (4.5:1+ on bg and white)
          rule: "#e5e5e5",   // borders / dividers
          bg: "#f3f3f5",     // page bg

          // Brand-mark tier (frontend/public/logo.svg, components/icons/index.tsx
          // OpenCheckIcon). Deliberately distinct from the UI tokens above --
          // the logo/mark has always used its own navy + blue pair, shipping
          // alongside the UI's oo.navy / oo.blue in production. Formalised
          // here (2026-07-23) so logo- and social-asset work (e.g.
          // outputs/mode-badges/) references named tokens instead of
          // re-hardcoding hex values. See CLAUDE.md "Brand: Check-mode badges".
          mark: {
            navy: "#0d1b3e",      // logo mark navy / badge background
            line: "#93c5fd",      // logo network-edge colour
            checkBlue: "#2563eb", // "Check" wordmark colour in logo.svg
          },
          // The logo's three fixed network-node colours (logo.svg + OpenCheckIcon).
          // Also the accent colour for each check-mode badge in
          // outputs/mode-badges/ -- one node colour per mode.
          node: {
            green: "#22c55e",  // QuickCheck badge accent
            blue: "#3b82f6",   // FullCheck badge accent
            purple: "#7c3aed", // BackgroundCheck badge accent
            // Climate & ESG, added Phase 122 for the fourth check mode. The
            // first node colour NOT lifted from logo.svg -- three modes had
            // three logo nodes, a fourth has none, and shipping a second
            // green next to `node.green` (#22c55e) would have been
            // indistinguishable in the mode tab strip.
            teal: "#0d9488",   // Climate & ESG badge accent
          },

          // Soft "selected / active" pair. Was #eef1fb + #cfd6f5 hardcoded 24x
          // across App, SubjectCard, NarrativePanel and ExportMenu.
          soft: "#eef1fb",
          softBorder: "#cfd6f5",

          // Semantic state tiers. Amber/emerald/rose were previously grabbed
          // ad hoc from the Tailwind palette per component, so the same hue
          // meant different things in different files. Named here so a
          // reviewer can see the meaning, not the swatch.
          warn: {
            bg: "#fffbeb",     // amber-50   -- incomplete, not failed
            border: "#fcd34d", // amber-300
            text: "#92400e",   // amber-800  (4.5:1+ on warn.bg)
          },
          ok: {
            bg: "#ecfdf5",     // emerald-50 -- corroborated / freely reusable
            border: "#a7f3d0", // emerald-200
            text: "#047857",   // emerald-700
          },
          info: {
            bg: "#f0f9ff",     // sky-50     -- structural context
            border: "#bae6fd", // sky-200
            text: "#0369a1",   // sky-700
          },

          // Ownership-graph relation palette. Ownership and role take the
          // FullCheck and BackgroundCheck node colours deliberately: the
          // network mode's accent is the ownership edge, and roles are held
          // by people, which is the people mode. Control keeps its orange --
          // the node tier has none, and control must stay distinguishable
          // from both. `*Text` values are the darker label colours needed for
          // 4.5:1 on white (WCAG 1.4.3); the line colours alone do not reach
          // it at text sizes.
          graph: {
            ownership: "#3b82f6",
            ownershipText: "#1d4ed8",
            ownershipTint: "#eff6ff",
            // The tint needs an edge when it is used as a surface rather than
            // a node fill -- the verdict strip's ownership-network invitation
            // is a card, and #eff6ff on white with no border is not one.
            ownershipTintBorder: "#bfdbfe",
            control: "#e65100",
            controlText: "#9a3412",
            controlTint: "#fdf0e8",
            role: "#7c3aed",
            roleText: "#6d28d9",
            roleTint: "#f5f3ff",
            same: "#b45309",
            unknown: "#888888",
            unknownText: "#595959",
          },
        },
      },
      fontFamily: {
        // Bitter for headings (consultancy brand match).
        head: ['Bitter', 'Georgia', 'serif'],
        // DM Sans for body / labels (the design system default).
        body: ['"DM Sans"', 'system-ui', 'sans-serif'],
        // DM Mono for indices, identifiers, link prefixes.
        mono: ['"DM Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
        // Tailwind defaults (sans/serif/mono) point at the body family
        // so existing utility classes keep working.
        sans: ['"DM Sans"', 'system-ui', 'sans-serif'],
        serif: ['Bitter', 'Georgia', 'serif'],
      },
      // The eight sizes the v2 design actually uses, named so a reviewer
      // reads intent rather than a pixel value, and so a later drift check
      // has a closed set to test against. Everything else in the codebase is
      // still a raw `text-[NNpx]` arbitrary value (520 of them across 26
      // files at Phase 122); those migrate component by component.
      fontSize: {
        "oo-meta": ["12px", { lineHeight: "1.5" }],   // captions, eyebrows, chips
        "oo-small": ["13px", { lineHeight: "1.6" }],  // secondary copy
        "oo-body": ["14px", { lineHeight: "1.6" }],   // body
        "oo-lead": ["15px", { lineHeight: "1.5" }],   // source headings
        "oo-head": ["16px", { lineHeight: "1.35" }],  // section headings
        "oo-stat": ["18px", { lineHeight: "1.3" }],   // verdict numerals
        "oo-title": ["22px", { lineHeight: "1.25" }], // subject name, phone
        "oo-display": ["26px", { lineHeight: "1.2" }],// subject name, desktop
      },
      borderRadius: {
        oo: "10px",
      },
      boxShadow: {
        "oo-card": "0 8px 32px rgba(61, 48, 212, 0.10)",
      },
      letterSpacing: {
        "oo-eyebrow": "0.12em",
      },
      maxWidth: {
        "oo-page": "1100px",
      },
    },
  },
  plugins: [],
};
