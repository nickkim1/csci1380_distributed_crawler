// @ts-check
/**
 * Distributed query and retrieval service.
 */

/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const distribution = globalThis.distribution;
const indexing = require("./util/indexing.js");

/**
 * @typedef {Object} QueryConfig
 * @property {string[]} [query]
 * @property {string} [text]
 * @property {string} [gid='all']
 * @property {string} [indexGid='all']
 * @property {number} [limit=10]
 * @property {boolean} [explain=false]
 */

/**
 * @param {Config} config
 * @returns {{exec: (configuration: QueryConfig, callback: Callback) => void}}
 */
function query(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * Execute a query against the distributed inverted index.
   * @param {QueryConfig} configuration
   * @param {Callback} callback
   */
  function exec(configuration, callback) {
    const gid = configuration.gid || context.gid;
    const indexGid = configuration.indexGid || gid;
    const limit = configuration.limit || 10;
    const parts = Array.isArray(configuration.query)
      ? configuration.query
      : (configuration.text || "").split(/\s+/).filter(Boolean);
    const analyzed = indexing.analyzeQuery(parts);
    const terms = Array.from(new Set(analyzed.terms));

    if (terms.length === 0) {
      return callback(null, []);
    }

    readMeta(indexGid, (metaError, meta) => {
      if (metaError) {
        return callback(metaError, null);
      }

      fetchTerms(indexGid, terms, (fetchError, termRecords) => {
        if (fetchError) {
          return callback(fetchError, null);
        }

        const results = rank(termRecords, meta, configuration.explain);
        callback(null, results.slice(0, limit));
      });
    });
  }

  /**
   * Read the shared index metadata.
   * @param {string} indexGid
   * @param {Callback} callback
   */
  function readMeta(indexGid, callback) {
    distribution[indexGid].store.get("__meta__", (error, meta) => {
      if (error && Object.keys(error).length) {
        return callback(null, { docCount: 0, termCount: 0 });
      }
      return callback(null, meta || { docCount: 0, termCount: 0 });
    });
  }

  /**
   * Fetch all index records for the query terms.
   * @param {string} indexGid
   * @param {string[]} terms
   * @param {Callback} callback
   */
  function fetchTerms(indexGid, terms, callback) {
    const termRecords = [];
    let remaining = terms.length;
    let failed = null;

    terms.forEach((term) => {
      distribution[indexGid].store.get(term, (error, record) => {
        if (!error || !Object.keys(error).length) {
          termRecords.push(record || { term, postings: [], df: 0 });
        } else {
          termRecords.push({ term, postings: [], df: 0 });
        }
        if (error && Object.keys(error).length && !failed) {
          failed = null;
        }
        remaining--;
        if (remaining === 0) {
          callback(failed, termRecords);
        }
      });
    });
  }

  /**
   * Score and rank documents.
   * @param {Array<{term: string, postings: Array<{url: string, tf: number, docLen: number, title: string}>, df: number}>} termRecords
   * @param {{docCount: number, termCount: number}} meta
   * @param {boolean | undefined} explain
   * @returns {Array<object>}
   */
  function rank(termRecords, meta, explain) {
    const docs = new Map();
    const docCount = Math.max(
      1,
      Number(meta && meta.docCount ? meta.docCount : 0),
    );

    termRecords.forEach((record) => {
      const postings = Array.isArray(record.postings) ? record.postings : [];
      const df = Math.max(1, Number(record.df || postings.length || 1));
      const idf = Math.log((docCount + 1) / (df + 1)) + 1;
      const phraseWeight = Math.max(1, record.term.split(" ").length);

      postings.forEach((posting) => {
        if (!posting || !posting.url) {
          return;
        }
        const doc = docs.get(posting.url) || {
          url: posting.url,
          score: 0,
          features: {
            terms: {},
            tfidf: 0,
            phraseBonus: 0,
            lengthNorm: 0,
          },
        };

        const lengthNorm =
          posting.docLen > 0
            ? posting.tf / Math.sqrt(posting.docLen)
            : posting.tf;
        const contribution = lengthNorm * idf * phraseWeight;
        doc.score += contribution;
        doc.features.terms[record.term] = {
          tf: posting.tf,
          df,
          idf,
          contribution,
        };
        doc.features.tfidf += lengthNorm * idf;
        doc.features.phraseBonus += phraseWeight > 1 ? contribution : 0;
        doc.features.lengthNorm += lengthNorm;
        if (!doc.title && posting.title) {
          doc.title = posting.title;
        }
        docs.set(posting.url, doc);
      });
    });

    return Array.from(docs.values())
      .sort((left, right) => {
        if (right.score !== left.score) {
          return right.score - left.score;
        }
        return left.url.localeCompare(right.url);
      })
      .map((doc) => {
        if (!explain) {
          return {
            url: doc.url,
            score: Number(doc.score.toFixed(6)),
          };
        }
        return {
          url: doc.url,
          score: Number(doc.score.toFixed(6)),
          title: doc.title || "",
          features: doc.features,
        };
      });
  }

  return { exec };
}

module.exports = query;
