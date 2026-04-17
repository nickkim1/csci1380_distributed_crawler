require("../../distribution.js")();
require("../helpers/sync-guard");

const path = require("path");
const { execFileSync } = require("child_process");

jest.setTimeout(180000);

function parseLastJson(stdout) {
  const text = String(stdout || "");
  const first = text.indexOf("{");
  const last = text.lastIndexOf("}");
  if (first === -1 || last === -1 || last < first) {
    throw new Error(`No JSON object found in output: ${text}`);
  }
  return JSON.parse(text.slice(first, last + 1));
}

test("(1 pts) student integration: sandbox3 e2e + curl validation + cache reuse", () => {
  const repoRoot = path.join(__dirname, "../..");
  const script = path.join(repoRoot, "scripts/m6_e2e_sandbox3_large.js");
  const cacheFile = path.join(repoRoot, ".cache", "m6_sandbox3_index_cache.integration.json");

  // Build and cache index.
  const firstRun = execFileSync(
    "node",
    [
      script,
      "--queries",
      "fiction",
      "--maxPages",
      "25",
      "--maxDepth",
      "2",
      "--cacheFile",
      cacheFile,
      "--refreshCache",
      "true",
    ],
    {
      cwd: repoRoot,
      encoding: "utf8",
      maxBuffer: 30 * 1024 * 1024,
    },
  );

  const buildOut = parseLastJson(firstRun);
  expect(buildOut).toHaveProperty("metrics.booksCrawled");
  expect(buildOut).toHaveProperty("metrics.booksIndexed");
  expect(buildOut).toHaveProperty("metrics.indexedTerms");
  expect(buildOut.metrics.booksCrawled).toBeGreaterThan(0);
  expect(buildOut.metrics.booksIndexed).toBeGreaterThan(0);
  expect(buildOut.metrics.indexedTerms).toBeGreaterThan(0);

  // Re-run with cache for speed.
  const secondRun = execFileSync(
    "node",
    [
      script,
      "--queries",
      "fiction",
      "--maxPages",
      "25",
      "--maxDepth",
      "2",
      "--cacheFile",
      cacheFile,
    ],
    {
      cwd: repoRoot,
      encoding: "utf8",
      maxBuffer: 30 * 1024 * 1024,
    },
  );

  const cachedOut = parseLastJson(secondRun);
  expect(cachedOut.usedCache).toBe(true);
  expect(Array.isArray(cachedOut.queryReports)).toBe(true);
  expect(cachedOut.queryReports[0].resultCount).toBeGreaterThan(0);

  const top = cachedOut.queryReports[0].topResults[0];
  expect(top).toBeTruthy();
  expect(typeof top.url).toBe("string");
  expect(top.url).toContain("/catalogue/");
  expect(top.url).not.toContain("/catalogue/category/");

  // Validate top result is fetchable and contains expected text with curl.
  const page = execFileSync(
    "curl",
    ["-skL", top.url],
    {
      cwd: repoRoot,
      encoding: "utf8",
      maxBuffer: 10 * 1024 * 1024,
    },
  );

  expect(page.length).toBeGreaterThan(100);
  expect(page.toLowerCase()).toContain("fiction");
});
