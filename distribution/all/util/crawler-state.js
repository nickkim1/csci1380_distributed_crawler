// @ts-check
/**
 * State inspection for distributed crawler
 * Monitors progress, detects cycles and pathological cases, provides diagnostics
 */

/**
 * @typedef {import("../../types.js").Callback} Callback
 */

const distribution = globalThis.distribution;
const urlUtils = require("./crawler-url-utils.js");

/**
 * Get comprehensive crawler state snapshot
 * @param {string} gid - Group ID
 * @param {Callback} callback
 */
function getState(gid, callback) {
  distribution.local.status.get("sid", (err, sid) => {
    if (err) {
      return callback(null, {
        status: "error",
        error: "Unable to get coordinator status",
      });
    }

    // Get all node statuses
    distribution[gid].status.get("sid", (err, nodes) => {
      const nodeCount = Object.keys(nodes || {}).length;

      callback(null, {
        coordinator: sid,
        nodeCount,
        nodes: nodes || {},
        timestamp: Date.now(),
      });
    });
  });
}

/**
 * Detect potential cycles by comparing round-to-round metrics
 * Returns null if no cycle detected, or details if cycle is suspected
 * @param {Object} prevMetrics
 * @param {Object} currMetrics
 * @returns {Object | null}
 */
function detectCycle(prevMetrics, currMetrics) {
  if (!prevMetrics) return null;

  const { visitedCount: prevVisited = 0, frontierCount: prevFrontier = 0 } =
    prevMetrics;
  const { visitedCount: currVisited = 0, frontierCount: currFrontier = 0 } =
    currMetrics;

  // If visited count didn't increase and frontier shrunk, possible cycle
  if (currVisited === prevVisited && currFrontier < prevFrontier) {
    return {
      type: "stalled_crawl",
      prevVisited,
      currVisited,
      prevFrontier,
      currFrontier,
      severity: currFrontier === 0 ? "resolved" : "ongoing",
    };
  }

  // If frontier is repeatedly the same, possible trap
  if (currFrontier === prevFrontier && currFrontier > 100) {
    return {
      type: "frontier_stagnation",
      frontierSize: currFrontier,
      severity: "warning",
    };
  }

  return null;
}

/**
 * Detect light-content corners (domains with very few pages discovered)
 * @param {Map} domainPageCounts - Map of domain -> page count
 * @param {number} threshold - Minimum pages per domain
 * @returns {string[]} List of low-content domains
 */
function detectLowContentDomains(domainPageCounts, threshold = 3) {
  const lowContent = [];
  domainPageCounts.forEach((count, domain) => {
    if (count < threshold) {
      lowContent.push(domain);
    }
  });
  return lowContent;
}

/**
 * Analyze crawl output patterns to detect scraping traps
 * @param {Object[]} crawlResults - Array of crawl result objects
 * @returns {Object} Trap analysis
 */
function analyzeTrapIndicators(crawlResults) {
  const trapIndicators = {
    sessionURLs: 0,
    dynamicGenerated: 0,
    infiniteDepth: 0,
    duplicateContent: 0,
    failureRate: 0,
  };

  if (!crawlResults || crawlResults.length === 0) {
    return trapIndicators;
  }

  const contentHashes = new Map();
  let failureCount = 0;

  crawlResults.forEach((result) => {
    if (result.failed) {
      failureCount++;
      return;
    }

    // Session/temp indicators
    if (/jsessionid|phpsessid|sid=|sessionid/i.test(result.url)) {
      trapIndicators.sessionURLs++;
    }

    // Dynamic generation (query string explosion)
    if ((result.url.match(/[?&]/g) || []).length > 10) {
      trapIndicators.dynamicGenerated++;
    }

    // Deep nesting
    if ((result.url.match(/\//g) || []).length > 10) {
      trapIndicators.infiniteDepth++;
    }

    // Duplicate content (same text hash)
    if (result.text) {
      const hash = simpleHash(result.text);
      contentHashes.set(hash, (contentHashes.get(hash) || 0) + 1);
    }
  });

  // Calculate duplicate rate
  const duplicates = Array.from(contentHashes.values()).filter(
    (count) => count > 1,
  );
  if (duplicates.length > 0) {
    trapIndicators.duplicateContent = duplicates.reduce((a, b) => a + b, 0);
  }

  trapIndicators.failureRate = failureCount / crawlResults.length;

  return trapIndicators;
}

/**
 * Proxy URL trap detection through the shared URL utility module.
 * @param {string} urlString
 * @returns {string | null}
 */
function detectTrap(urlString) {
  return urlUtils.detectTrap(urlString);
}

/**
 * Generate diagnostic report for crawler health
 * @param {Object} stats - Current crawler stats
 * @param {Object} metrics - Round metrics
 * @param {Object} trapAnalysis - Trap indicator analysis
 * @returns {Object} Diagnostic report
 */
function generateDiagnostics(stats, metrics, trapAnalysis) {
  const report = {
    status: "healthy",
    alerts: [],
    warnings: [],
    metrics: {
      ...stats,
      ...metrics,
    },
    trapAnalysis,
  };

  // Check for pathological patterns
  if (trapAnalysis.failureRate > 0.3) {
    report.alerts.push("High failure rate detected (>30%)");
    report.status = "degraded";
  }

  if (trapAnalysis.sessionURLs > stats.totalPagesVisited * 0.1) {
    report.warnings.push("High proportion of session-based URLs");
  }

  if (trapAnalysis.dynamicGenerated > stats.totalPagesVisited * 0.2) {
    report.warnings.push("Many dynamically generated URLs detected");
  }

  if (trapAnalysis.duplicateContent > stats.totalPagesVisited * 0.15) {
    report.warnings.push("High duplicate content rate");
  }

  if (stats.cyclesDetected > 0) {
    report.alerts.push(`${stats.cyclesDetected} potential cycles detected`);
  }

  if (metrics.frontierCount === 0 && stats.totalPagesVisited === 0) {
    report.alerts.push("Crawl did not start - check seed URLs");
    report.status = "error";
  }

  return report;
}

/**
 * Simple hash function for content comparison
 * @private
 */
function simpleHash(text) {
  if (!text) return "empty";

  let hash = 0;
  for (let i = 0; i < Math.min(text.length, 1000); i++) {
    const char = text.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return hash.toString(36);
}

module.exports = {
  getState,
  detectCycle,
  detectLowContentDomains,
  detectTrap,
  analyzeTrapIndicators,
  generateDiagnostics,
};
