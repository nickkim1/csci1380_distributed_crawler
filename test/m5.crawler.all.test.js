// @ts-nocheck
/**
 * Test: Distributed Crawler Integration
 *
 * Tests the distributed crawler service built on M5 MapReduce.
 * Validates:
 * - URL canonicalization and deduplication
 * - Frontier management and persistence
 * - Trap detection and pathological case handling
 * - Round-based crawl execution
 * - State inspection and metrics
 */

require("../distribution.js")();
require("./helpers/sync-guard");
const distribution = globalThis.distribution;

const urlUtils = require("../distribution/all/util/crawler-url-utils.js");
const frontierMgr = require("../distribution/all/util/crawler-frontier.js");
const stateInspector = require("../distribution/all/util/crawler-state.js");

const n1 = { ip: "127.0.0.1", port: 7150 };
const n2 = { ip: "127.0.0.1", port: 7151 };
const n3 = { ip: "127.0.0.1", port: 7152 };

describe("Distributed Crawler", () => {
  beforeEach((done) => {
    // Setup 3-node group for testing
    distribution.node.start(() => {
      distribution.local.status.spawn(n1, (e1) => {
        distribution.local.status.spawn(n2, (e2) => {
          distribution.local.status.spawn(n3, (e3) => {
            const group = {};
            distribution.local.groups.get("local", (e, local) => {
              Object.assign(group, local);
              distribution.local.groups.put(group, "crawlerTest", (e) => {
                done();
              });
            });
          });
        });
      });
    });
  });

  afterEach((done) => {
    distribution.local.status.get("sid", (e, v) => {
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: n1 },
        (e1) => {
          distribution.local.comm.send(
            [],
            { service: "status", method: "stop", node: n2 },
            (e2) => {
              distribution.local.comm.send(
                [],
                { service: "status", method: "stop", node: n3 },
                (e3) => {
                  distribution.local.status.stop(done);
                },
              );
            },
          );
        },
      );
    });
  });

  test("(10 pts) crawler-url-utils:canonicalize", () => {
    const testCases = [
      {
        input: "HTTPS://Example.Com:443/Path",
        expected: "https://example.com/Path",
      },
      {
        input: "http://example.com:80/path/",
        expected: "http://example.com/path/",
      },
      {
        input: "https://example.com/path#anchor",
        expected: "https://example.com/path",
      },
      {
        input: "https://example.com/path?b=2&a=1",
        expected: "https://example.com/path?a=1&b=2",
      },
    ];

    testCases.forEach((tc) => {
      const result = urlUtils.canonicalize(tc.input);
      expect(result).toEqual(tc.expected);
    });
  });

  test("(10 pts) crawler-url-utils:trap-detection", () => {
    const trapCases = [
      {
        url: "https://example.com/session/JSESSIONID=12345/page",
        trap: "session_or_temp_path",
      },
      {
        url: "https://example.com/path?a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9&j=10&k=11&l=12&m=13&n=14&o=15&p=16",
        trap: "excessive_parameters",
      },
      {
        url: "https://example.com/2024/01/15/archive/page",
        trap: "session_or_temp_path",
      },
    ];

    trapCases.forEach((tc) => {
      const result = stateInspector.detectTrap(tc.url);
      expect(result).toEqual(tc.trap);
    });

    // Valid URLs should not trap
    const validURL = "https://example.com/valid/path/to/content";
    expect(stateInspector.detectTrap(validURL)).toBeNull();
  });

  test("(10 pts) crawler-url-utils:valid-crawl-target", () => {
    const validTargets = [
      "https://example.com/page",
      "http://example.com/page",
      "https://example.com/path/to/resource",
    ];

    const invalidTargets = [
      "https://example.com/image.jpg",
      "https://example.com/style.css",
      "https://example.com/script.js",
      "ftp://example.com/file",
      "https://example.com/path?id=1&id=2&id=3&id=4&id=5&id=6&id=7&id=8&id=9&id=10&id=11&id=12&id=13&id=14&id=15&id=16&id=17",
    ];

    validTargets.forEach((url) => {
      expect(urlUtils.isValidCrawlTarget(url)).toBe(true);
    });

    invalidTargets.forEach((url) => {
      expect(urlUtils.isValidCrawlTarget(url)).toBe(false);
    });
  });

  test("(10 pts) crawler-url-utils:domain-extraction", () => {
    const testCases = [
      {
        url: "https://example.com/path",
        expected: "example.com",
      },
      {
        url: "https://SUB.Example.Com/path",
        expected: "sub.example.com",
      },
      {
        url: "http://localhost:8080/page",
        expected: "localhost",
      },
    ];

    testCases.forEach((tc) => {
      const result = urlUtils.getDomain(tc.url);
      expect(result).toEqual(tc.expected);
    });
  });

  test("(10 pts) crawler-frontier:cycle-detection", () => {
    const prev = {
      visitedCount: 100,
      frontierCount: 50,
    };

    const curr1 = {
      visitedCount: 100, // No progress
      frontierCount: 30, // Frontier shrunk
    };

    const cycle1 = stateInspector.detectCycle(prev, curr1);
    expect(cycle1).not.toBeNull();
    expect(cycle1.type).toEqual("stalled_crawl");

    const curr2 = {
      visitedCount: 150, // Progress made
      frontierCount: 40,
    };

    const cycle2 = stateInspector.detectCycle(prev, curr2);
    expect(cycle2).toBeNull(); // No cycle with progress
  });

  test("(10 pts) crawler-state:trap-analysis", () => {
    const results = [
      { url: "https://example.com/page1", text: "content1", failed: false },
      { url: "https://example.com/page2", text: "content2", failed: false },
      { url: "https://example.com/page3", text: "content2", failed: false }, // Duplicate content
      {
        url: "https://example.com/JSESSIONID=123/page4",
        text: "content4",
        failed: false,
      },
      { url: "https://example.com/error", failed: true }, // Failed crawl
    ];

    const analysis = stateInspector.analyzeTrapIndicators(results);

    expect(analysis.failureRate).toBeGreaterThan(0);
    expect(analysis.sessionURLs).toBeGreaterThan(0);
    expect(analysis.duplicateContent).toBeGreaterThan(0);
  });

  test("(10 pts) crawler-state:diagnostics", () => {
    const stats = {
      totalPagesVisited: 100,
      totalURLsDiscovered: 250,
      failedURLs: 2,
      duplicatesFiltered: 5,
      roundsCompleted: 3,
      cyclesDetected: 0,
    };

    const metrics = {
      visitedCount: 100,
      frontierCount: 75,
    };

    const trapAnalysis = {
      sessionURLs: 5,
      dynamicGenerated: 8,
      infiniteDepth: 0,
      duplicateContent: 3,
      failureRate: 0.02,
    };

    const report = stateInspector.generateDiagnostics(
      stats,
      metrics,
      trapAnalysis,
    );

    expect(report.status).toEqual("healthy");
    expect(report.metrics.totalPagesVisited).toEqual(100);
    expect(Array.isArray(report.alerts)).toBe(true);
    expect(Array.isArray(report.warnings)).toBe(true);
  });
});
