// @ts-check
/**
 * Lean distributed crawler.
 *
 * The crawler only keeps the state it needs:
 * - frontier manifests
 * - seen URL markers
 * - crawled document manifests
 * - persisted document bodies for later indexing
 */

/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const { execFileSync } = require("child_process");
const path = require("path");

const distribution = globalThis.distribution;
const id = distribution.util.id;
const urlUtils = require("./util/crawler-url-utils.js");
const frontier = require("./util/crawler-frontier.js");

const curlCommand = "curl";
const curlArgs = [
  "-skL",
  "--retry",
  "3",
  "--retry-delay",
  "1",
  "--retry-connrefused",
];
const getURLsScript = path.join(
  __dirname,
  "../../non-distribution/c/getURLs.js",
);
const getTextScript = path.join(
  __dirname,
  "../../non-distribution/c/getText.js",
);

/**
 * @typedef {Object} CrawlerConfig
 * @property {string[]} seedURLs
 * @property {number} [maxDepth=3]
 * @property {number} [maxPages=100000]
 * @property {string} [gid='all']
 */

/**
 * @param {Config} config
 * @returns {{exec: (configuration: CrawlerConfig, callback: Callback) => void, state: (callback: Callback) => void}}
 */
function crawler(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * @param {CrawlerConfig} configuration
   * @param {Callback} callback
   */
  function exec(configuration, callback) {
    const seedURLs = Array.isArray(configuration.seedURLs)
      ? configuration.seedURLs
      : [];
    if (seedURLs.length === 0) {
      return callback(Error("crawler: no seed URLs provided"), null);
    }

    const crawlID = id.getID(`crawl_${Date.now()}`);
    const crawlConfig = {
      gid: configuration.gid || context.gid,
      maxDepth: configuration.maxDepth || 3,
      maxPages: configuration.maxPages || 100000,
    };

    frontier.init(crawlID, seedURLs, crawlConfig, (initError) => {
      if (initError) {
        return callback(initError, null);
      }
      runRound(crawlID, crawlConfig, callback);
    });
  }

  /**
   * Crawl rounds continue until the frontier is empty or the depth/page budget is exhausted.
   * @param {string} crawlID
   * @param {{gid: string, maxDepth: number, maxPages: number}} crawlConfig
   * @param {Callback} callback
   * @param {number} [pagesSeen=0]
   */
  function runRound(crawlID, crawlConfig, callback, pagesSeen = 0) {
    frontier.getFrontier(crawlID, crawlConfig, (error, entries) => {
      if (error) {
        return callback(error, null);
      }

      const currentEntries = (entries || []).filter(
        (entry) => entry && entry.url,
      );
      if (currentEntries.length === 0 || pagesSeen >= crawlConfig.maxPages) {
        return callback(null, {
          crawlID,
          docsManifestKey: frontier.manifestKey(crawlID, "docs"),
          frontierManifestKey: frontier.manifestKey(
            crawlID,
            "frontier-entries",
          ),
        });
      }

      const roundKeys = currentEntries.map((entry) =>
        frontier.frontierKey(crawlID, entry.url),
      );
      distribution[crawlConfig.gid].mr.exec(
        {
          keys: roundKeys,
          map: crawlMap(crawlID, crawlConfig),
          reduce: crawlReduce(crawlID),
        },
        (mrError, results) => {
          if (mrError) {
            return callback(mrError, null);
          }

          persistRound(
            crawlID,
            crawlConfig,
            currentEntries,
            results || [],
            pagesSeen,
            callback,
          );
        },
      );
    });
  }

  /**
   * Build the synchronous mapper used by MR.
   * It returns one document record plus one candidate record per discovered URL.
   * @param {string} crawlID
   * @param {{gid: string, maxDepth: number, maxPages: number}} crawlConfig
   */
  function crawlMap(crawlID, crawlConfig) {
    return function map(_key, frontierEntry) {
      const entry = frontierEntry || {};
      const url = urlUtils.canonicalize(entry.url || _key);
      const depth = Number(entry.depth || 0);

      if (!urlUtils.isValidCrawlTarget(url) || urlUtils.detectTrap(url)) {
        return [
          {
            [frontier.docKey(crawlID, url)]: {
              type: "doc",
              url,
              failed: true,
              depth,
              text: "",
              title: "",
              outlinks: [],
            },
          },
        ];
      }

      try {
        const html = execFileSync(curlCommand, curlArgs.concat(url), {
          encoding: "utf-8",
          maxBuffer: 20 * 1024 * 1024,
        });
        const outlinksOutput = execFileSync(getURLsScript, {
          encoding: "utf-8",
          input: html,
          maxBuffer: 20 * 1024 * 1024,
        });
        const textOutput = execFileSync(getTextScript, {
          encoding: "utf-8",
          input: html,
          maxBuffer: 20 * 1024 * 1024,
        });

        const rawOutlinks = outlinksOutput
          .split(/\r?\n/)
          .map((line) => line.trim())
          .filter(Boolean);
        const outlinks = [];
        const outlinkSeen = new Set();
        rawOutlinks.forEach((href) => {
          const canonical = urlUtils.canonicalize(href);
          if (!urlUtils.isValidCrawlTarget(canonical)) {
            return;
          }
          if (urlUtils.detectTrap(canonical)) {
            return;
          }
          if (!outlinkSeen.has(canonical)) {
            outlinkSeen.add(canonical);
            outlinks.push(canonical);
          }
        });

        const document = {
          type: "doc",
          url,
          depth,
          title: "",
          text: textOutput,
          outlinks,
        };

        /** @type {any[]} */
        const output = [
          {
            [frontier.docKey(crawlID, url)]: document,
          },
        ];

        outlinks.forEach((candidateURL) => {
          output.push({
            [frontier.frontierKey(crawlID, candidateURL)]: {
              type: "frontier",
              url: candidateURL,
              depth: Math.min(depth + 1, crawlConfig.maxDepth),
              parent: url,
            },
          });
        });

        return output;
      } catch (fetchError) {
        return [
          {
            [frontier.docKey(crawlID, url)]: {
              type: "doc",
              url,
              failed: true,
              depth,
              text: "",
              title: "",
              outlinks: [],
              error:
                fetchError && fetchError.message
                  ? fetchError.message
                  : String(fetchError),
            },
          },
        ];
      }
    };
  }

  /**
   * The reduce phase just groups identical candidate URLs and keeps one representative.
   * @param {string} crawlID
   */
  function crawlReduce(crawlID) {
    return function reduce(key, values) {
      const entries = Array.isArray(values) ? values : [];
      if (key.startsWith(`crawler:${crawlID}:doc:`)) {
        return entries[0];
      }

      const parents = new Set();
      let depth = Infinity;
      let url = key.slice(`crawler:${crawlID}:frontier:`.length);

      entries.forEach((entry) => {
        if (!entry) {
          return;
        }
        if (entry.parent) {
          parents.add(entry.parent);
        }
        if (Number.isFinite(entry.depth)) {
          depth = Math.min(depth, entry.depth);
        }
        if (entry.url) {
          url = entry.url;
        }
      });

      return {
        type: "frontier",
        url,
        depth: Number.isFinite(depth) ? depth : 0,
        parents: Array.from(parents),
      };
    };
  }

  /**
   * Persist the current round's results and advance to the next frontier.
   * @param {string} crawlID
   * @param {{gid: string, maxDepth: number, maxPages: number}} crawlConfig
   * @param {Array<object>} roundResults
   * @param {number} pagesSeen
   * @param {Callback} callback
   */
  function persistRound(
    crawlID,
    crawlConfig,
    _currentEntries,
    roundResults,
    pagesSeen,
    callback,
  ) {
    const docs = [];
    const candidates = [];

    roundResults.forEach((result) => {
      if (!result || typeof result !== "object") {
        return;
      }
      const values = Object.values(result);
      const value = values[0];
      if (!value) {
        return;
      }
      if (value.type === "doc" && !value.failed) {
        docs.push(value);
      } else if (value.type === "frontier") {
        candidates.push(value);
      }
    });

    const docUrls = docs.map((doc) => doc.url);
    const nextFrontierMap = new Map();
    let pendingDocs = docs.length;
    let pendingCandidates = candidates.length;
    let finished = false;
    let processedDocs = 0;
    let docsManifestReady = docs.length === 0;

    const finish = (error, value) => {
      if (finished) {
        return;
      }
      finished = true;
      callback(error, value);
    };

    const maybeAdvance = () => {
      if (pendingDocs > 0 || pendingCandidates > 0 || !docsManifestReady) {
        return;
      }

      const nextFrontier = Array.from(nextFrontierMap.values())
        .filter((entry) => entry.depth <= crawlConfig.maxDepth)
        .slice(
          0,
          Math.max(0, crawlConfig.maxPages - (pagesSeen + processedDocs)),
        );

      if (nextFrontier.length === 0) {
        return frontier.setFrontier(crawlID, crawlConfig, [], (error) => {
          if (error) {
            return finish(error, null);
          }
          return finish(null, {
            crawlID,
            docsManifestKey: frontier.manifestKey(crawlID, "docs"),
            frontierManifestKey: frontier.manifestKey(
              crawlID,
              "frontier-entries",
            ),
          });
        });
      }

      frontier.setFrontier(crawlID, crawlConfig, nextFrontier, (error) => {
        if (error) {
          return finish(error, null);
        }
        runRound(crawlID, crawlConfig, finish, pagesSeen + processedDocs);
      });
    };

    docs.forEach((doc) => {
      frontier.putDoc(crawlID, crawlConfig, doc, (docError) => {
        pendingDocs--;
        if (!docError) {
          processedDocs++;
        }
        if (docError) {
          return finish(docError, null);
        }
        maybeAdvance();
      });
    });

    if (docs.length === 0) {
      pendingDocs = 0;
      maybeAdvance();
      return;
    }

    candidates.forEach((candidate) => {
      const candidateURL = urlUtils.canonicalize(candidate.url);
      if (candidate.depth > crawlConfig.maxDepth) {
        pendingCandidates--;
        maybeAdvance();
        return;
      }

      frontier.hasSeen(
        crawlID,
        crawlConfig,
        candidateURL,
        (seenError, seen) => {
          if (seenError) {
            pendingCandidates--;
            return finish(seenError, null);
          }

          if (!seen) {
            frontier.markSeen(
              crawlID,
              crawlConfig,
              candidateURL,
              (markError) => {
                if (markError) {
                  pendingCandidates--;
                  return finish(markError, null);
                }
                const entry = {
                  url: candidateURL,
                  depth: candidate.depth,
                  parent:
                    candidate.parents && candidate.parents.length
                      ? candidate.parents[0]
                      : null,
                };
                nextFrontierMap.set(candidateURL, entry);
                pendingCandidates--;
                maybeAdvance();
              },
            );
            return;
          }

          pendingCandidates--;
          maybeAdvance();
        },
      );
    });

    if (candidates.length === 0) {
      pendingCandidates = 0;
      maybeAdvance();
    }

    frontier.appendDocs(crawlID, crawlConfig, docUrls, (appendError) => {
      if (appendError) {
        return finish(appendError, null);
      }
      docsManifestReady = true;
      maybeAdvance();
    });
  }

  /**
   * Minimal state view: only the persistent manifests that matter for the pipeline.
   * @param {Callback} callback
   */
  function state(callback) {
    callback(null, {
      crawlID: null,
      frontierManifestKey: null,
      docsManifestKey: null,
    });
  }

  return { exec, state };
}

module.exports = crawler;
