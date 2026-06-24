/**
 * Changelog generator — copies docs/status.md into a JSON the frontend bundles.
 *
 * The /changelog page is generated from the repo's development-history table in
 * docs/status.md: every phase row becomes a changelog entry. This script does the
 * dumb part (read the markdown, write it into src/lib/statusMarkdown.json); the
 * actual parsing lives in src/lib/changelog.ts so it can be unit-tested.
 *
 * Runs automatically via the `prebuild` npm hook, so each deploy picks up the
 * latest status.md. Re-run by hand with `npm run gen:changelog`.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const statusPath = fileURLToPath(new URL("../../docs/status.md", import.meta.url));
const outPath = fileURLToPath(new URL("../src/lib/statusMarkdown.json", import.meta.url));

const markdown = readFileSync(statusPath, "utf8");
writeFileSync(
  outPath,
  JSON.stringify({ markdown, generatedAt: new Date().toISOString() }, null, 2) + "\n",
);
console.log(`changelog: wrote ${outPath} (${markdown.length} chars from docs/status.md)`);
