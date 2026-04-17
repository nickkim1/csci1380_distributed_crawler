// @ts-check
/**
 * Distributed Crawler - Reference Implementation
 * Complete example with actual HTML parsing and URL extraction
 *
 * This is a reference showing how to integrate with real HTTP fetching
 * and HTML parsing using jsdom (already available in the project via getURLs.js)
 */

/**
 * Actual mapper implementation that crawls URLs
 * This would replace the stub in distribution/all/crawler.js
 *
 * @param {string} url - URL to crawl
 * @param {Object} context - {timeout, crawlID, mrID}
 * @returns {Object[]} Array of result objects
 */
function crawlURLActual(url, context) {
  // In production, use:
  // - node-fetch or built-in fetch for HTTP
  // - jsdom or cheerio for HTML parsing
  // - custom extractors for URL and text extraction

  // This is pseudocode showing the structure:
  /*
  try {
    const timeout = context.timeout || 30000;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'DistributedCrawler/1.0 (+http://example.com/bot)',
      },
      redirect: 'follow',
      timeout: timeout,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      return [{
        url,
        failed: true,
        error: `HTTP ${response.status}`,
        timestamp: Date.now(),
      }];
    }

    const contentType = response.headers.get('content-type');
    if (!contentType || !contentType.includes('text/html')) {
      return [{
        url,
        failed: true,
        error: 'not-html',
        timestamp: Date.now(),
      }];
    }

    const html = await response.text();
    const {JSDOM} = require('jsdom');
    const dom = new JSDOM(html, {url, prettyPrintTagNames: true});
    const {window} = dom;
    const {document} = window;

    // Extract outlinks
    const outlinks = [];
    const seen = new Set();
    document.querySelectorAll('a[href]').forEach((a) => {
      try {
        const href = a.getAttribute('href');
        const resolved = new URL(href, url).toString();
        if (!seen.has(resolved)) {
          outlinks.push(resolved);
          seen.add(resolved);
        }
      } catch (e) {
        // Skip invalid URLs
      }
    });

    // Extract text content
    const text = document.body?.textContent || '';

    return [{
      url,
      outlinks,
      text: text.substring(0, 100000), // Limit text size
      title: document.title || '',
      contentLength: html.length,
      timestamp: Date.now(),
      headers: {
        'content-type': contentType,
        'content-length': html.length,
      },
    }];
  } catch (e) {
    return [{
      url,
      failed: true,
      error: e.message,
      timestamp: Date.now(),
    }];
  }
  */

  // Stub for testing - would be replaced with actual fetch + parse
  return [
    {
      url,
      outlinks: [],
      text: "",
      timestamp: Date.now(),
    },
  ];
}

/**
 * Enhanced mapper for distributed crawling
 * Handles per-node work partitioning and local caching
 */
function createEnhancedMapper(context) {
  const urlUtils = require("./util/crawler-url-utils.js");
  const localCache = new Map();
  const maxCacheSize = 1000;

  return function enhancedMapper(url, _unused) {
    // Check local cache first (avoid re-crawling on same node)
    const canonical = urlUtils.canonicalize(url);
    if (localCache.has(canonical)) {
      return [localCache.get(canonical)];
    }

    // Detect trap patterns before fetching
    const trap = urlUtils.detectTrap(canonical);
    if (trap) {
      const result = {
        url: canonical,
        failed: true,
        error: `trap_detected:${trap}`,
        timestamp: Date.now(),
      };
      cacheResult(canonical, result);
      return [result];
    }

    // Validate crawl target
    if (!urlUtils.isValidCrawlTarget(canonical)) {
      const result = {
        url: canonical,
        failed: true,
        error: "not_valid_target",
        timestamp: Date.now(),
      };
      cacheResult(canonical, result);
      return [result];
    }

    // Perform actual crawl
    const result = crawlURLActual(canonical, context);

    // Cache result
    if (result && result[0]) {
      cacheResult(canonical, result[0]);
    }

    return result;
  };

  function cacheResult(url, result) {
    if (localCache.size >= maxCacheSize) {
      // Simple eviction: remove first entry
      const firstKey = localCache.keys().next().value;
      localCache.delete(firstKey);
    }
    localCache.set(url, result);
  }
}

/**
 * Distributed indexing mapper
 * Processes crawled documents for inverted index construction
 *
 * Mirrors non-distribution/index.sh pipeline:
 * process -> stem -> combine -> invert
 */
function createIndexMapper() {
  // Requires integration with:
  // - Porter stemmer (c/stem.js)
  // - N-gram generator (c/combine.js)
  // - Inverted index builder (c/invert.js)

  return function indexMapper(docId, docContent) {
    /*
    const terms = [];

    // Process: normalize, lowercase, remove stopwords
    const normalized = docContent
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter((word) => word.length > 2 && !isStopword(word));

    // Stem (would call c/stem.js equivalent)
    const stemmed = normalized.map((word) => porterStem(word));

    // Combine: generate 1-, 2-, 3-grams
    stemmed.forEach((term) => {
      terms.push(term); // 1-gram
    });
    for (let i = 0; i < stemmed.length - 1; i++) {
      terms.push(`${stemmed[i]} ${stemmed[i + 1]}`); // 2-gram
      if (i < stemmed.length - 2) {
        terms.push(`${stemmed[i]} ${stemmed[i + 1]} ${stemmed[i + 2]}`); // 3-gram
      }
    }

    // Invert: emit term -> docId mappings
    const inverted = terms.map((term) => ({
      [term]: docId,
    }));

    return inverted;
    */

    // Stub for testing
    return [{ term_count: (docContent || "").split(/\s+/).length }];
  };
}

/**
 * Distributed reducer for merging partial indices
 *
 * Combines term postings from multiple nodes
 */
function createIndexReducer() {
  return function indexReducer(term, docIds) {
    return {
      term,
      documents: Array.from(new Set(docIds)),
      docCount: new Set(docIds).size,
      idfScore: Math.log(1000 / (new Set(docIds).size + 1)), // Simple IDF approximation
    };
  };
}

module.exports = {
  crawlURLActual,
  createEnhancedMapper,
  createIndexMapper,
  createIndexReducer,
};
