// @ts-check

/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const fs = require("node:fs");
const path = require("node:path");

const distribution = globalThis.distribution;

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

/**
 * @param {string} word
 * @returns {string}
 */
function stem(word) {
  // Keep query-time stemming aligned with index-time stemming.
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
  // Normalize user input into the same token space used by the index.
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
 * @param {Config} config
 */
function query(config) {
  const context = {
    gid: config.gid || "all",
    hash: config.hash || distribution.util.id.naiveHash,
  };

  /**
   * @param {{query: string|string[], terms?: string[], indexGid?: string, limit?: number}} configuration
   * @param {Callback} callback
   */
  function exec(configuration, callback) {
    // Query defaults to the index gid written by crawler.exec.
    const indexGid = configuration?.indexGid || `index_${context.gid}`;
    const limit = Number.isInteger(configuration?.limit)
      ? configuration.limit
      : 20;

    const raw = Array.isArray(configuration?.query)
      ? configuration.query.join(" ")
      : Array.isArray(configuration?.terms)
        ? configuration.terms.join(" ")
        : String(configuration?.query || "");

    const normalized = normalizeTerms(raw);
    if (normalized.length === 0) {
      return callback(null, { query: raw, terms: [], results: [] });
    }

    const searchTerms = [
      // Try whole phrase first, then individual terms, with duplicates removed.
      normalized.join(" "),
      ...normalized,
    ].filter((t, i, arr) => t && arr.indexOf(t) === i);

    const scores = new Map();
    distribution.local.groups.get(context.gid, (groupErr, group) => {
      if (groupErr) {
        return callback(groupErr, null);
      }

      const nids = Object.keys(group);
      if (nids.length === 0) {
        return callback(Error("query.exec: empty group"), null);
      }

      /** @type {Map<string, string[]>} */
      const termsByNid = new Map();
      searchTerms.forEach((term) => {
        const nid = context.hash(distribution.util.id.getID(term), nids);
        const existing = termsByNid.get(nid) || [];
        existing.push(term);
        termsByNid.set(nid, existing);
      });

      let pendingShards = termsByNid.size;

      const done = () => {
        // Rank by aggregate score across all matched terms/phrases.
        const results = Array.from(scores.entries())
          .map(([url, score]) => ({ url, score }))
          .sort((a, b) => b.score - a.score || a.url.localeCompare(b.url))
          .slice(0, limit);

        callback(null, {
          query: raw,
          terms: searchTerms,
          results,
        });
      };

      if (pendingShards === 0) {
        return done();
      }

      termsByNid.forEach((terms, nid) => {
        const node = group[nid];
        const sid = distribution.util.id.getSID(node);
        const fileKey = `inv_${indexGid}_${sid}`;

        // Fetch shard file from the owning node, then score only requested terms.
        distribution.local.comm.send(
          [{ key: fileKey, gid: indexGid }],
          { node, service: "store", method: "get", gid: "local" },
          (err, shard) => {
            if (!err && shard && typeof shard === "object") {
              terms.forEach((term) => {
                const postings = shard[term];
                if (!Array.isArray(postings)) {
                  return;
                }
                postings.forEach((p) => {
                  if (!p || !p.url) {
                    return;
                  }
                  scores.set(p.url, (scores.get(p.url) || 0) + (p.count || 0));
                });
              });
            }

            pendingShards--;
            if (pendingShards === 0) {
              done();
            }
          },
        );
      });
    });
  }

  return { exec };
}

module.exports = query;
