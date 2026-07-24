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
