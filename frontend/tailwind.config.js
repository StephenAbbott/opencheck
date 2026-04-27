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
          muted: "#757575",  // secondary copy
          rule: "#e5e5e5",   // borders / dividers
          bg: "#f3f3f5",     // page bg
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
