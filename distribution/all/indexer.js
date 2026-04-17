// @ts-check
/**
 * Distributed inverted index builder.
 */

/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const distribution = globalThis.distribution;
const frontier = require("./util/crawler-frontier.js");
const indexing = require("./util/indexing.js");

/**
 * @typedef {Object} IndexerConfig
 * @property {string} [crawlID]
 * @property {string[]} [keys]
 * @property {string} [gid='all']
 * @property {string} [indexGid='all']
 */

/**
 * @param {Config} config
 * @returns {{exec: (configuration: IndexerConfig, callback: Callback) => void}}
 */
function indexer(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * Build an inverted index from stored crawl documents.
   * @param {IndexerConfig} configuration
   * @param {Callback} callback
   */
  function exec(configuration, callback) {
    const gid = configuration.gid || context.gid;
    const indexGid = configuration.indexGid || gid;
    const crawlID = configuration.crawlID || "";
    const docKeys = Array.isArray(configuration.keys)
      ? configuration.keys.slice()
      : [];
    const manifestKey = crawlID ? frontier.manifestKey(crawlID, "docs") : null;

    if (docKeys.length === 0 && manifestKey) {
      return distribution[gid].store.get(manifestKey, (error, keys) => {
        if (error && Object.keys(error).length) {
          return callback(error, null);
        }
        runIndex(Array.isArray(keys) ? keys : []);
      });
    }

    return runIndex(docKeys);

    function runIndex(keys) {
      if (keys.length === 0) {
        return callback(null, []);
      }

      const normalizedKeys = Array.from(new Set(keys)).map((key) => {
        if (key.startsWith(`crawler:${crawlID}:doc:`)) {
          return key;
        }
        return crawlID ? frontier.docKey(crawlID, key) : key;
      });
      distribution[gid].mr.exec(
        {
          keys: normalizedKeys,
          map: mapDoc(crawlID, gid),
          reduce: reduceTerms(),
        },
        (mrError, batchResults) => {
          if (mrError) {
            return callback(mrError, null);
          }

          persistIndexBatch(
            indexGid,
            batchResults || [],
            normalizedKeys.length,
            callback,
          );
        },
      );
    }
  }

  /**
   * Map each stored document to term-frequency postings.
   * @param {string} crawlID
   * @param {string} gid
   */
  function mapDoc(crawlID, gid) {
    return function map(key, storedDoc) {
      const doc = storedDoc || {};
      const url = doc.url || key;
      const text = doc.text || "";
      const title = doc.title || "";
      const analysis = indexing.analyzeText(text);
      const postings = [];
      const seen = new Set();

      analysis.counts.forEach((tf, term) => {
        if (seen.has(term)) {
          return;
        }
        seen.add(term);
        postings.push({
          [term]: {
            type: "posting",
            term,
            url,
            tf,
            docLen: analysis.docLength,
            title,
            crawlID,
          },
        });
      });

      return postings;
    };
  }

  /**
   * Merge postings for the same term inside a single MR batch.
   */
  function reduceTerms() {
    return function reduce(term, values) {
      const postings = [];
      const byUrl = new Map();
      (values || []).forEach((value) => {
        if (!value || !value.url) {
          return;
        }
        const current = byUrl.get(value.url) || {
          url: value.url,
          tf: 0,
          docLen: value.docLen || 0,
          title: value.title || "",
        };
        current.tf += Number(value.tf || 0);
        current.docLen = Math.max(current.docLen, Number(value.docLen || 0));
        if (!current.title && value.title) {
          current.title = value.title;
        }
        byUrl.set(value.url, current);
      });

      Array.from(byUrl.values())
        .sort((left, right) => {
          if (right.tf !== left.tf) {
            return right.tf - left.tf;
          }
          return left.url.localeCompare(right.url);
        })
        .forEach((posting) => postings.push(posting));

      return {
        type: "term",
        term,
        postings,
        df: postings.length,
      };
    };
  }

  /**
   * Persist a batch of term postings into the distributed index store.
   * @param {string} indexGid
   * @param {Array<object>} batchResults
   * @param {number} docCountIncrement
   * @param {Callback} callback
   */
  function persistIndexBatch(
    indexGid,
    batchResults,
    docCountIncrement,
    callback,
  ) {
    const termRecords = [];
    batchResults.forEach((result) => {
      if (!result || typeof result !== "object") {
        return;
      }
      const value = Object.values(result)[0];
      if (value && value.type === "term") {
        termRecords.push(value);
      }
    });

    let remaining = termRecords.length;
    if (remaining === 0) {
      return updateMeta(indexGid, docCountIncrement, 0, callback);
    }

    let failed = null;
    termRecords.forEach((record) => {
      distribution[indexGid].store.get(record.term, (error, existing) => {
        const merged = mergeTermRecord(record, existing);
        distribution[indexGid].store.put(
          merged,
          { key: record.term, gid: indexGid },
          (putError) => {
            if (putError && !failed) {
              failed = putError;
            }
            remaining--;
            if (remaining === 0) {
              updateMeta(
                indexGid,
                docCountIncrement,
                termRecords.length,
                (metaError) => {
                  callback(failed || metaError || null, termRecords);
                },
              );
            }
          },
        );
      });
    });
  }

  /**
   * Merge a fresh term record with any existing persisted record.
   * @param {{term: string, postings: Array<{url: string, tf: number, docLen: number, title: string}>, df: number}} fresh
   * @param {any} existing
   */
  function mergeTermRecord(fresh, existing) {
    const postings = new Map();
    const existingPostings =
      existing && Array.isArray(existing.postings) ? existing.postings : [];
    existingPostings.forEach((posting) => {
      if (!posting || !posting.url) {
        return;
      }
      postings.set(posting.url, {
        url: posting.url,
        tf: Number(posting.tf || 0),
        docLen: Number(posting.docLen || 0),
        title: posting.title || "",
      });
    });

    fresh.postings.forEach((posting) => {
      const current = postings.get(posting.url) || {
        url: posting.url,
        tf: 0,
        docLen: Number(posting.docLen || 0),
        title: posting.title || "",
      };
      current.tf += Number(posting.tf || 0);
      current.docLen = Math.max(current.docLen, Number(posting.docLen || 0));
      if (!current.title && posting.title) {
        current.title = posting.title;
      }
      postings.set(posting.url, current);
    });

    const mergedPostings = Array.from(postings.values()).sort((left, right) => {
      if (right.tf !== left.tf) {
        return right.tf - left.tf;
      }
      return left.url.localeCompare(right.url);
    });

    return {
      term: fresh.term,
      postings: mergedPostings,
      df: mergedPostings.length,
    };
  }

  /**
   * Update aggregate index metadata.
   * @param {string} indexGid
   * @param {number} addedDocs
   * @param {number} addedTerms
   * @param {Callback} callback
   */
  function updateMeta(indexGid, addedDocs, addedTerms, callback) {
    const metaKey = "__meta__";
    distribution[indexGid].store.get(metaKey, (error, meta) => {
      const nextMeta = {
        docCount: Number(meta && meta.docCount ? meta.docCount : 0),
        termCount: Number(meta && meta.termCount ? meta.termCount : 0),
        updatedAt: Date.now(),
      };
      nextMeta.docCount += addedDocs;
      nextMeta.termCount += addedTerms;
      distribution[indexGid].store.put(
        nextMeta,
        { key: metaKey, gid: indexGid },
        (putError) => {
          callback(putError || null, nextMeta);
        },
      );
    });
  }

  return { exec };
}

module.exports = indexer;
