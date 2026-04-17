require("../../distribution.js")();
require("../helpers/sync-guard");

const path = require("path");
const { execFileSync } = require("child_process");

jest.setTimeout(180000);

const repoRoot = path.join(__dirname, "../..");
const script = path.join(repoRoot, "scripts/m6_e2e_sandbox3_large.js");

function parseLastJson(stdout) {
  const text = String(stdout || "");
  const first = text.indexOf("{");
  const last = text.lastIndexOf("}");
  if (first === -1 || last === -1 || last < first) {
    throw new Error(`No JSON object found in output: ${text}`);
  }
  return JSON.parse(text.slice(first, last + 1));
}

function runSandboxE2E(args) {
  const out = execFileSync("node", [script, ...args], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 30 * 1024 * 1024,
  });
  return parseLastJson(out);
}

test("(1 pts) student integration: sandbox3 e2e + curl validation + cache reuse", () => {
  const cacheFile = path.join(repoRoot, ".cache", "m6_sandbox3_index_cache.integration.json");

  // Build and cache index.
  const buildOut = runSandboxE2E([
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
    ]);
  expect(buildOut).toHaveProperty("metrics.booksCrawled");
  expect(buildOut).toHaveProperty("metrics.booksIndexed");
  expect(buildOut).toHaveProperty("metrics.indexedTerms");
  expect(buildOut.metrics.booksCrawled).toBeGreaterThan(0);
  expect(buildOut.metrics.booksIndexed).toBeGreaterThan(0);
  expect(buildOut.metrics.indexedTerms).toBeGreaterThan(0);

  // Re-run with cache for speed.
  const cachedOut = runSandboxE2E([
      "--queries",
      "fiction",
      "--maxPages",
      "25",
      "--maxDepth",
      "2",
      "--cacheFile",
      cacheFile,
    ]);
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

test("(1 pts) student integration: sandbox3 e2e supports multiple queries with book-only results", () => {
  const cacheFile = path.join(repoRoot, ".cache", "m6_sandbox3_index_cache.multi-query.json");
  const out = runSandboxE2E([
    "--queries",
    "fiction,mystery,poetry",
    "--maxPages",
    "60",
    "--maxDepth",
    "4",
    "--cacheFile",
    cacheFile,
    "--refreshCache",
    "true",
  ]);

  expect(out.usedCache).toBe(false);
  expect(out.metrics.booksIndexed).toBeGreaterThan(0);
  expect(out.metrics.indexedTerms).toBeGreaterThan(0);
  expect(Array.isArray(out.queryReports)).toBe(true);
  expect(out.queryReports.length).toBe(3);

  out.queryReports.forEach((report) => {
    expect(typeof report.query).toBe("string");
    expect(report.resultCount).toBeGreaterThan(0);
    expect(Array.isArray(report.topResults)).toBe(true);
    expect(report.topResults.length).toBeGreaterThan(0);

    const urls = report.topResults.map((r) => r.url);
    const uniqueUrls = new Set(urls);
    expect(uniqueUrls.size).toBe(urls.length);

    report.topResults.forEach((row) => {
      expect(typeof row.url).toBe("string");
      expect(row.url).toContain("/catalogue/");
      expect(row.url).not.toContain("/catalogue/category/");
      expect(typeof row.score).toBe("number");
      expect(row.score).toBeGreaterThan(0);
    });
  });
});

test("(1 pts) student integration: sandbox3 cache reuse keeps index size stable", () => {
  const cacheFile = path.join(repoRoot, ".cache", "m6_sandbox3_index_cache.stability.json");

  const buildOut = runSandboxE2E([
    "--queries",
    "history",
    "--maxPages",
    "80",
    "--maxDepth",
    "4",
    "--cacheFile",
    cacheFile,
    "--refreshCache",
    "true",
  ]);

  const cachedOut = runSandboxE2E([
    "--queries",
    "history",
    "--maxPages",
    "80",
    "--maxDepth",
    "4",
    "--cacheFile",
    cacheFile,
  ]);

  expect(buildOut.usedCache).toBe(false);
  expect(cachedOut.usedCache).toBe(true);

  expect(buildOut.metrics.booksIndexed).toBeGreaterThan(0);
  expect(cachedOut.metrics.booksIndexed).toBe(buildOut.metrics.booksIndexed);
  expect(cachedOut.metrics.indexedTerms).toBe(buildOut.metrics.indexedTerms);

  expect(Array.isArray(cachedOut.queryReports)).toBe(true);
  expect(cachedOut.queryReports[0].resultCount).toBeGreaterThan(0);
  expect(cachedOut.queryReports[0].topResults[0].url).toContain("/catalogue/");
  expect(cachedOut.queryReports[0].topResults[0].url).not.toContain("/catalogue/category/");
});
