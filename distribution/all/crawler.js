// @ts-check

/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const distribution = globalThis.distribution;
const id = distribution.util.id;
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const STOPWORDS_PATH = path.join(
  __dirname,
  "../../non-distribution/d/stopwords.txt",
);
const STOPWORDS = new Set(
  (fs.existsSync(STOPWORDS_PATH) ? fs.readFileSync(STOPWORDS_PATH, "utf8") : "")
    .split(/\r?\n/)
    .map((w) => w.trim())
    .filter(Boolean),
);

const ASSET_EXT_RE = /\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|map|pdf|zip|gz|mp3|mp4|avi|mov)(\?|#|$)/i;

/**
 * @param {string} maybeURL
 * @returns {boolean}
 */
function shouldCrawlURL(maybeURL) {
  if (!maybeURL || typeof maybeURL !== "string") {
    return false;
  }
  try {
    const parsed = new globalThis.URL(maybeURL);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return false;
    }
    if (ASSET_EXT_RE.test(parsed.pathname)) {
      return false;
    }
    return true;
  } catch (_error) {
    return false;
  }
}

/**
 * @param {string} maybeURL
 * @returns {boolean}
 */
function isBookDetailURL(maybeURL) {
  if (!maybeURL || typeof maybeURL !== "string") {
    return false;
  }
  try {
    const parsed = new globalThis.URL(maybeURL);
    const pathName = parsed.pathname || "";
    const hostName = (parsed.hostname || "").toLowerCase();

    // Books-to-Scrape sandbox detail pages.
    if (pathName.includes("/catalogue/")) {
      if (pathName.includes("/catalogue/category/")) {
        return false;
      }
      return /_[0-9]+\/index\.html$/i.test(pathName);
    }

    // Atlas Gutenberg mirror: treat text/html book files as indexable docs.
    if (hostName === "atlas.cs.brown.edu" && pathName.startsWith("/data/gutenberg/")) {
      const fileName = pathName.split("/").pop() || "";
      if (!fileName || fileName.endsWith("/")) {
        return false;
      }
      if (["books.txt", "indextree.txt", "donate-howto.txt"].includes(fileName.toLowerCase())) {
        return false;
      }
      const isTextLike = /\.txt(?:\.[a-z0-9-]+)?$/i.test(fileName);
      const isHTMLLike = /\.x?html?$/i.test(fileName);
      if (!(isTextLike || isHTMLLike)) {
        return false;
      }
      // Prefer actual ebook files, which almost always include an ID.
      return /\d/.test(fileName);
    }

    return false;
  } catch (_error) {
    return false;
  }
}

/**
 * @param {string} url
 * @returns {string}
 */
function fetchHTML(url) {
  // MR map/reduce functions are synchronous in this codebase, so fetch inline.
  const out = spawnSync(
    "curl",
    ["-skL", "--retry", "2", "--retry-delay", "1", "--retry-connrefused", url],
    { encoding: "utf8" },
  );
  if (out.status !== 0 || !out.stdout) {
    return "";
  }
  return out.stdout;
}

/**
 * @param {string} html
 * @returns {string}
 */
function htmlToText(html) {
  // Keep extraction lightweight: strip tags/scripts/styles and normalize whitespace.
  return html
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, " ")
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} word
 * @returns {string}
 */
function stem(word) {
  // Small stemmer to stay dependency-light while still merging common inflections.
  if (word.length > 4 && word.endsWith("ing")) {
    return word.slice(0, -3);
  }
  if (word.length > 3 && (word.endsWith("ed") || word.endsWith("es"))) {
    return word.slice(0, -2);
  }
  if (word.length > 2 && word.endsWith("s")) {
    return word.slice(0, -1);
  }
  return word;
}

/**
 * @param {string} text
 * @returns {string[]}
 */
function normalizeTerms(text) {
  // Match the non-distributed flow: lowercase, alpha-only tokens, stopword removal, stem.
  return text
    .toLowerCase()
    .replace(/[^a-z]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter((w) => w.length > 0 && !STOPWORDS.has(w))
    .map(stem)
    .filter(Boolean);
}

/**
 * @param {string[]} terms
 * @returns {string[]}
 */
function makeNgrams(terms) {
  // Emit 1/2/3-grams so querying can match phrases as in the original indexer.
  const out = [];
  for (let i = 0; i < terms.length; i++) {
    out.push(terms[i]);
    if (i + 1 < terms.length) {
      out.push(`${terms[i]} ${terms[i + 1]}`);
    }
    if (i + 2 < terms.length) {
      out.push(`${terms[i]} ${terms[i + 1]} ${terms[i + 2]}`);
    }
  }
  return out;
}

/**
 * @param {string} baseURL
 * @param {string} html
 * @returns {string[]}
 */
function extractURLs(baseURL, html) {
  // Resolve relative links against the source page and deduplicate on a set.
  const links = new Set([baseURL]);
  const re = /<a\s+[^>]*href=["']([^"']+)["'][^>]*>/gi;
  let match = re.exec(html);
  while (match) {
    const href = match[1];
    try {
      const next = new globalThis.URL(href, baseURL).href;
      if (shouldCrawlURL(next)) {
        links.add(next);
      }
    } catch (e) {
      // Ignore malformed links.
    }
    match = re.exec(html);
  }
  return Array.from(links);
}

/**
 * @typedef {Object} CrawlerConfig
 * This should probably just be a struct that is of the form:
 *  { urls: [] }
 * where [] is just a list of seed URLs to start crawling.
 */

function crawler(config) {
  const context = {
    gid: config.gid || "all",
    hash: config.hash || id.naiveHash,
  };

  /**
   * @param {CrawlerConfig} configuration
   * @param {Callback} callback
   * @returns {void}
   *
   * First MR exec flow:
   * 1) Setup: first, parse out the seed URLs from the configuration object.
   * Don't bother with error checking right now for malformed URLs, but that should
   * be a todo for a later point.
   * 2) MR: take those seed URLs and pass them to the mr exec function.
   *
   * mapper: this should be a function that is of the form:
   * (key : hash(url), value : url) => {
   * return [{key : hash(outgoing url), value : outgoing url} ...] (note though this is
   * flattened before added to the out results!!! ALSO here the result should NOT be emitted
   * IF it's already present in the visited set (check the global cross-node visited file set))
   * }
   *
   * reducer: this should be a function that is of the form:
   * (key : hash(outgoing url), value : [outgoing url1, outgoing url2...] ) = {
   * return {key : hash(outgoing url), value: text for outgoing url}
   * }
   *
   * In the map phase proper:
   *    a) Crawl the HTML text for anchor tags/other links (similar to getURLs.js)
   *    b) Get all the URLs and collect them into a list of outgoing objects
   *    c) In principle, just takes in 1 tuple -> outputs (potentially) many tuples
   *    NOTE: the map results will be stored into a single file with name like: <sid>map<gid>
   *    as an array of mapped objects/results, per the existing implementation.
   *
   * In the shuffle phase proper:
   *    Based on the existing implementation, this will fetch all the mapped obj results and for each of them
   *    it'll call local.mem.append() to collate them by unique key (in this case, the hash of the outgoing url).
   *    And put each unique URL on its own node for the reducer to run, since the operative assumption is for all vals
   *    for a given URL to live on same (to-be reducer) node.
   *
   * In the reducer phase proper:
   *    Based on the existing implementation, it'll just get all the unique keys from mem.get
   *    then run mem.get to get the corresponding values for each uq key then will call the reducer
   *    function on these. Then it'll return the node-level results back up to the group mr call which will collate these
   *    by node into a node map blah blah.
   *
   * 3) Store the reduced results into persistent storage i.e., in smth like: <sid>reduce<gid>
   * as a list of reduced objects: [{ key: hash(outgoing url), value: text for outgoing url}, {...}... ]
   *
   * Second MR exec flow
   * 1) mapper: (key : hash(outgoing url), value: text for outgoing url) -> { key: term, value : <outgoing url> <count of term in outgoing url> }
   * 2) shuffle - groups by hash(term) and sends to appropriate node via consistent hashing.
   * 3) reducer: ( key : term, value : [<outgoing url> <count of term in outgoing url>, ...] ) ->
   *    { key : term, value : sorted([<outgoing url> <count of term in outgoing url>, ...]) }
   *
   * Query service:
   *    The end of this second reducer stage should be post-processed such that each node stores a small text
   *    file as an inverted index that we can subsequently query. A separate query service will chunk
   *    the query into component terms, hash them, then route them to the corresponding node which will
   *    theoretically for the same term hash have a bunch of terms mapping to it to comprise an overall
   *    inverted index (e.g,. if hash(apple) -> 4 and hash(me) -> 4 then the inverted index will comprise
   *    {apple : ...}, {me : ... }, etc. ). Then the query service can collect the relevant results at
   *    the end, sort them, and present them back to the user. I guess you can retrofit the query service
   *    to MR but that defeats the point, querying isn't a data processing service it's supposed to be fast
   *    and it's a read-only (not write) task which MR isn't.
   *
   */
  function exec(configuration, callback) {
    // Seed URLs are treated as the input dataset for the first MR job.
    const urls = Array.isArray(configuration?.urls) ? configuration.urls : [];
    const maxPages = Number.isInteger(configuration?.maxPages)
      ? Math.max(1, Number(configuration.maxPages))
      : 200;
    if (urls.length === 0) {
      return callback(
        Error("crawler.exec: configuration.urls must be a non-empty array"),
      );
    }

    const runId = id.getID({ urls, now: Date.now() }).slice(0, 12);
    const crawlDataGid = context.gid;
    const indexGid = configuration.indexGid || `index_${context.gid}`;

    const crawlStats = {
      seeds: urls.length,
      mrDocs: 0,
      fallbackUsed: false,
      fallbackDocs: 0,
      pagesFetched: 0,
      bookDocs: 0,
      docsWithTerms: 0,
      sampleDocs: [],
      crawlMs: 0,
      indexMs: 0,
    };

    const crawlStart = Date.now();

    const seedKeys = urls.map((u, i) => `seed_${runId}_${i}`);
    let pendingSeeds = seedKeys.length;
    let failed = false;

    const onSeedStored = (err) => {
      if (failed) {
        return;
      }
      if (err) {
        failed = true;
        return callback(err, null);
      }
      pendingSeeds--;
      if (pendingSeeds === 0) {
        runCrawlMR();
      }
    };

    seedKeys.forEach((key, i) => {
      distribution[context.gid].store.put(
        urls[i],
        { key, gid: crawlDataGid },
        onSeedStored,
      );
    });

    function runCrawlMR() {
      // MR #1: discover URLs (map) and materialize crawlable docs {url, text} (reduce).
      distribution[context.gid].mr.exec(
        {
          keys: seedKeys,
          map: (_key, url) => {
            if (!shouldCrawlURL(url)) {
              return [];
            }
            const html = fetchHTML(url);
            if (!html) {
              return [];
            }
            const discovered = extractURLs(url, html);
            return discovered.map((nextURL) => ({
              [id.getID(nextURL)]: nextURL,
            }));
          },
          reduce: (_urlHash, values) => {
            // Shuffle already groups by URL hash; reduce picks one URL and fetches page text.
            const unique = Array.from(new Set(values || []));
            const chosen = unique[0];
            if (!chosen) {
              return null;
            }
            const html = fetchHTML(chosen);
            if (!html) {
              return null;
            }
            return {
              key: id.getID(chosen),
              value: {
                url: chosen,
                text: htmlToText(html),
              },
            };
          },
        },
        (crawlErr, crawlDocsRaw) => {
          if (crawlErr) {
            return callback(crawlErr, null);
          }
          let crawlDocs = (crawlDocsRaw || []).filter(
            (doc) =>
              doc &&
              doc.key &&
              doc.value &&
              doc.value.url &&
              isBookDetailURL(doc.value.url),
          );
          crawlStats.mrDocs = crawlDocs.length;

          if (crawlDocs.length === 0) {
            crawlStats.fallbackUsed = true;
            crawlDocs = fallbackCrawl(urls, maxPages);
            crawlStats.fallbackDocs = crawlDocs.length;
          }

          crawlStats.bookDocs = crawlDocs.length;
          crawlStats.crawlMs = Date.now() - crawlStart;

          if (crawlDocs.length === 0) {
            return callback(null, {
              docs: 0,
              terms: 0,
              indexGid,
              crawlStats,
            });
          }

          stageDocsAndIndex(crawlDocs);
        },
      );
    }

    /**
     * @param {Array<{key: string, value: {url: string, text: string}}>} crawlDocs
     */
    function stageDocsAndIndex(crawlDocs) {
      const docKeys = crawlDocs.map((doc) => doc.key);
      let pendingDocs = crawlDocs.length;
      let docFailure = false;

      const afterDocStore = (err) => {
        if (docFailure) {
          return;
        }
        if (err) {
          docFailure = true;
          return callback(err, null);
        }
        pendingDocs--;
        if (pendingDocs === 0) {
          runIndexLocal(crawlDocs, docKeys);
        }
      };

      crawlDocs.forEach((doc) => {
        distribution[context.gid].store.put(
          doc.value,
          { key: doc.key, gid: crawlDataGid },
          afterDocStore,
        );
      });
    }

    /**
     * Fallback local crawl if MR crawl yields no docs.
     * @param {string[]} seedURLs
     * @param {number} pageBudget
     * @returns {Array<{key: string, value: {url: string, text: string}}>} docs
     */
    function fallbackCrawl(seedURLs, pageBudget) {
      const queue = [];
      const visited = new Set();
      const docs = [];
      const crawlBudget = Math.max(pageBudget * 20, 4000);

      seedURLs.forEach((url) => {
        if (shouldCrawlURL(url) && !visited.has(url)) {
          queue.push(url);
          visited.add(url);
        }
      });

      while (
        queue.length > 0 &&
        docs.length < pageBudget &&
        crawlStats.pagesFetched < crawlBudget
      ) {
        const current = queue.shift();
        const html = fetchHTML(current);
        if (!html) {
          continue;
        }

        crawlStats.pagesFetched++;

        if (crawlStats.pagesFetched % 10 === 0) {
          globalThis.console.log(
            `[crawler] progress pages=${crawlStats.pagesFetched} books=${docs.length} queue=${queue.length}`,
          );
        }

        if (isBookDetailURL(current)) {
          docs.push({
            key: id.getID(current),
            value: {
              url: current,
              text: htmlToText(html),
            },
          });
        }

        const discovered = extractURLs(current, html);
        for (const next of discovered) {
          if (!visited.has(next) && shouldCrawlURL(next)) {
            visited.add(next);
            queue.push(next);
            if (visited.size > pageBudget * 10) {
              break;
            }
          }
        }
      }

      return docs;
    }

    function runIndexLocal(crawlDocs, docKeys) {
      const indexStart = Date.now();
      /** @type {Map<string, Map<string, number>>} */
      const postingsByTerm = new Map();

      crawlDocs.forEach((doc, idx) => {
        const url = doc?.value?.url;
        const text = doc?.value?.text || "";
        if (!url) {
          return;
        }

        const terms = makeNgrams(normalizeTerms(text));
        const counts = new Map();
        terms.forEach((term) => counts.set(term, (counts.get(term) || 0) + 1));

        if (counts.size > 0) {
          crawlStats.docsWithTerms++;
        }

        if (idx < 3) {
          crawlStats.sampleDocs.push({
            url,
            textChars: text.length,
            uniqueTerms: counts.size,
            topTerms: Array.from(counts.entries())
              .sort((a, b) => b[1] - a[1])
              .slice(0, 5),
          });
        }

        counts.forEach((count, term) => {
          const perURL = postingsByTerm.get(term) || new Map();
          perURL.set(url, (perURL.get(url) || 0) + count);
          postingsByTerm.set(term, perURL);
        });
      });

      const inverted = Array.from(postingsByTerm.entries()).map(
        ([term, perURL]) => ({
          key: term,
          value: Array.from(perURL.entries())
            .map(([url, count]) => ({ url, count }))
            .sort((a, b) => b.count - a.count || a.url.localeCompare(b.url)),
        }),
      );

      if (inverted.length === 0) {
        crawlStats.indexMs = Date.now() - indexStart;
        return callback(null, {
          docs: docKeys.length,
          terms: 0,
          indexGid,
          crawlStats,
        });
      }

      distribution.local.groups.get(context.gid, (groupErr, group) => {
        if (groupErr) {
          return callback(groupErr, null);
        }

        const nids = Object.keys(group);
        if (nids.length === 0) {
          return callback(Error("crawler.exec: empty group"), null);
        }

        /** @type {Object.<string, Object.<string, any[]>>} */
        const shardByNid = {};
        inverted.forEach((row) => {
          const term = row.key;
          const nid = context.hash(id.getID(term), nids);
          if (!shardByNid[nid]) {
            shardByNid[nid] = {};
          }
          shardByNid[nid][term] = row.value;
        });

        const targetNids = Object.keys(shardByNid);
        let pendingShards = targetNids.length;
        let shardFailure = false;

        if (pendingShards === 0) {
          return callback(null, {
            docs: docKeys.length,
            terms: 0,
            indexGid,
            files: {},
            crawlStats,
          });
        }

        const files = {};
        targetNids.forEach((nid) => {
          const node = group[nid];
          const sid = id.getSID(node);
          const fileKey = `inv_${indexGid}_${sid}`;
          files[sid] = fileKey;

          distribution.local.comm.send(
            [shardByNid[nid], { key: fileKey, gid: indexGid }],
            { node, service: "store", method: "put", gid: "local" },
            (persistErr) => {
              if (shardFailure) {
                return;
              }
              if (persistErr) {
                shardFailure = true;
                return callback(persistErr, null);
              }

              pendingShards--;
              if (pendingShards === 0) {
                crawlStats.indexMs = Date.now() - indexStart;
                return callback(null, {
                  docs: docKeys.length,
                  terms: inverted.length,
                  indexGid,
                  files,
                  crawlStats,
                });
              }
            },
          );
        });
      });
    }
  }

  return { exec };
}

module.exports = crawler;
