// @ts-check
/**
 * URL utilities for distributed crawler
 * Handles canonicalization, domain extraction, and duplicate detection
 */

/**
 * Canonicalize a URL for comparison and deduplication
 * Normalizes scheme, host, path, removes fragments, standardizes port
 * @param {string} urlString
 * @returns {string} Canonical URL
 */
function canonicalize(urlString) {
  try {
    const url = new URL(urlString);

    // Normalize scheme to lowercase
    url.protocol = url.protocol.toLowerCase();

    // Normalize host to lowercase, remove www prefix for consistency
    let host = url.hostname.toLowerCase();
    // Optional: remove www prefix for grouping similar sites
    // host = host.replace(/^www\./, '');
    url.hostname = host;

    // Remove default ports (80 for http, 443 for https)
    if (
      (url.protocol === "http:" && url.port === "80") ||
      (url.protocol === "https:" && url.port === "443")
    ) {
      url.port = "";
    }

    // Normalize path: remove trailing slash from root, decode unreserved chars
    let pathname = url.pathname || "/";
    if (pathname === "//") pathname = "/";
    url.pathname = pathname;

    // Remove fragment (hash)
    url.hash = "";

    // Sort query parameters for consistent ordering
    if (url.search) {
      const params = new URLSearchParams(url.search);
      const sorted = new URLSearchParams([...params].sort());
      url.search = sorted.toString();
    }

    // Return canonical form
    let canonical = url.toString();
    // Remove trailing slash from non-root paths (optional, for consistency)
    if (canonical.endsWith("/") && canonical.indexOf("/", 8) > 8) {
      // Keep trailing slash for now; adjust policy as needed
    }

    return canonical;
  } catch (e) {
    // If URL parsing fails, return original
    return urlString;
  }
}

/**
 * Extract domain from URL for politeness/throttling decisions
 * @param {string} urlString
 * @returns {string} Domain (e.g., "example.com")
 */
function getDomain(urlString) {
  try {
    const url = new URL(urlString);
    return url.hostname.toLowerCase();
  } catch (e) {
    return "";
  }
}

/**
 * Extract scheme from URL
 * @param {string} urlString
 * @returns {string} Scheme (e.g., "http", "https")
 */
function getScheme(urlString) {
  try {
    const url = new URL(urlString);
    return url.protocol.replace(":", "").toLowerCase();
  } catch (e) {
    return "";
  }
}

/**
 * Check if URL is valid and should be crawled
 * @param {string} urlString
 * @returns {boolean}
 */
function isValidCrawlTarget(urlString) {
  try {
    const url = new URL(urlString);

    // Only crawl http/https
    if (!["http:", "https:"].includes(url.protocol)) {
      return false;
    }

    // Reject common non-content URLs
    const path = url.pathname.toLowerCase();
    const invalidPatterns = [
      /\.(jpg|jpeg|png|gif|pdf|zip|exe|dmg|iso)$/i,
      /\.(css|js|json|xml|svg|ico|woff|woff2|ttf|eot)$/i,
      /\.(mp3|mp4|mpeg|avi|mov|flv|webm)$/i,
      /robots\.txt/i,
      /sitemap\.xml/i,
      /\.well-known\//i,
    ];

    for (const pattern of invalidPatterns) {
      if (pattern.test(path)) {
        return false;
      }
    }

    // Reject extremely long URLs (potential trap indicators)
    if (urlString.length > 2000) {
      return false;
    }

    return true;
  } catch (e) {
    return false;
  }
}

/**
 * Resolve relative URL to absolute using base URL
 * @param {string} relativeURL
 * @param {string} baseURL
 * @returns {string} Absolute URL
 */
function resolveURL(relativeURL, baseURL) {
  try {
    return new URL(relativeURL, baseURL).toString();
  } catch (e) {
    return null;
  }
}

/**
 * Detect potential scraping traps
 * Returns reason if URL matches trap patterns, null otherwise
 * @param {string} urlString
 * @returns {string | null}
 */
function detectTrap(urlString) {
  try {
    const url = new URL(urlString);
    const path = url.pathname.toLowerCase();
    const fullUrl = url.toString().toLowerCase();

    // Deeply nested paths often indicate traps
    const slashCount = (path.match(/\//g) || []).length;
    if (slashCount > 10) {
      return "excessive_nesting";
    }

    // Session/temp directories
    if (/session|tmp|temp|cache|archive|backup|old|deprecated/i.test(path)) {
      return "session_or_temp_path";
    }

    // Print/export URLs with session IDs
    if (
      /jsessionid|phpsessid|sid=|sessionid|print|export|format=pdf/i.test(
        fullUrl,
      )
    ) {
      return "session_or_export_url";
    }

    // Dynamic parameter explosion patterns (common in pagination traps)
    const queryCount = (url.search.match(/[&?]/g) || []).length;
    if (queryCount > 15) {
      return "excessive_parameters";
    }

    // Datetime-based URLs that could generate infinite variations
    if (/\/\d{4}\/\d{2}\/\d{2}\/.*\?/i.test(fullUrl)) {
      return "datetime_pagination_pattern";
    }

    return null;
  } catch (e) {
    return null;
  }
}

/**
 * Get URL distance metric for BFS-style crawling
 * Simple heuristic: count slashes in path (depth)
 * @param {string} urlString
 * @returns {number}
 */
function getURLDepth(urlString) {
  try {
    const url = new URL(urlString);
    const path = url.pathname;
    return (path.match(/\//g) || []).length - 1; // -1 to not count leading slash
  } catch (e) {
    return 0;
  }
}

module.exports = {
  canonicalize,
  getDomain,
  getScheme,
  isValidCrawlTarget,
  resolveURL,
  detectTrap,
  getURLDepth,
};
