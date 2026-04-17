// @ts-check
/**
 * Persistent frontier helpers for the distributed crawler.
 */

/**
 * @typedef {import("../../types.js").Callback} Callback
 */

const distribution = globalThis.distribution;
const urlUtils = require("./crawler-url-utils.js");

/**
 * Build the key used to store a frontier entry.
 * @param {string} crawlID
 * @param {string} url
 * @returns {string}
 */
function frontierKey(crawlID, url) {
  return `crawler:${crawlID}:frontier:${url}`;
}

/**
 * Build the key used to store a crawled document.
 * @param {string} crawlID
 * @param {string} url
 * @returns {string}
 */
function docKey(crawlID, url) {
  return `crawler:${crawlID}:doc:${url}`;
}

/**
 * Build the key used to mark a URL as seen.
 * @param {string} crawlID
 * @param {string} url
 * @returns {string}
 */
function seenKey(crawlID, url) {
  return `crawler:${crawlID}:seen:${url}`;
}

/**
 * Build the key used to store a manifest array.
 * @param {string} crawlID
 * @param {string} name
 * @returns {string}
 */
function manifestKey(crawlID, name) {
  return `crawler:${crawlID}:${name}`;
}

/**
 * Initialize persistent crawl state.
 * @param {string} crawlID
 * @param {string[]} seedURLs
 * @param {object} config
 * @param {Callback} callback
 */
function init(crawlID, seedURLs, config, callback) {
  const gid = config.gid;
  const frontierEntries = [];
  const frontierKeys = [];
  const pending = [];

  seedURLs.forEach((url) => {
    if (!urlUtils.isValidCrawlTarget(url)) {
      return;
    }
    const canonical = urlUtils.canonicalize(url);
    frontierEntries.push({ url: canonical, depth: 0, parent: null });
    frontierKeys.push(frontierKey(crawlID, canonical));
    pending.push((done) =>
      putValue(gid, seenKey(crawlID, canonical), true, done),
    );
  });

  pending.push((done) =>
    writeManifest(
      gid,
      manifestKey(crawlID, "frontier-keys"),
      frontierKeys,
      done,
    ),
  );
  pending.push((done) =>
    writeManifest(
      gid,
      manifestKey(crawlID, "frontier-entries"),
      frontierEntries,
      done,
    ),
  );
  pending.push((done) =>
    writeManifest(gid, manifestKey(crawlID, "docs"), [], done),
  );
  pending.push((done) =>
    writeManifest(gid, manifestKey(crawlID, "next-frontier"), [], done),
  );

  settleAll(pending, callback);
}

/**
 * Read the current frontier entries from the manifest.
 * @param {string} crawlID
 * @param {object} config
 * @param {Callback} callback
 */
function getFrontier(crawlID, config, callback) {
  readManifest(config.gid, manifestKey(crawlID, "frontier-entries"), callback);
}

/**
 * Persist the frontier entries for the next round.
 * @param {string} crawlID
 * @param {object} config
 * @param {Array<{url: string, depth: number, parent: string | null}>} entries
 * @param {Callback} callback
 */
function setFrontier(crawlID, config, entries, callback) {
  const gid = config.gid;
  const frontierKeys = entries.map((entry) => frontierKey(crawlID, entry.url));
  const pending = [];

  entries.forEach((entry) => {
    pending.push((done) =>
      putValue(gid, frontierKey(crawlID, entry.url), entry, done),
    );
  });
  pending.push((done) =>
    writeManifest(gid, manifestKey(crawlID, "frontier-entries"), entries, done),
  );
  pending.push((done) =>
    writeManifest(
      gid,
      manifestKey(crawlID, "frontier-keys"),
      frontierKeys,
      done,
    ),
  );

  settleAll(pending, callback);
}

/**
 * Append new document URLs to the crawl manifest.
 * @param {string} crawlID
 * @param {object} config
 * @param {string[]} urls
 * @param {Callback} callback
 */
function appendDocs(crawlID, config, urls, callback) {
  readManifest(config.gid, manifestKey(crawlID, "docs"), (error, docs) => {
    if (error) {
      return callback(error);
    }
    const seen = new Set(docs);
    urls.forEach((url) => seen.add(url));
    writeManifest(
      config.gid,
      manifestKey(crawlID, "docs"),
      Array.from(seen),
      callback,
    );
  });
}

/**
 * Mark a URL as seen.
 * @param {string} crawlID
 * @param {object} config
 * @param {string} url
 * @param {Callback} callback
 */
function markSeen(crawlID, config, url, callback) {
  putValue(config.gid, seenKey(crawlID, url), true, callback);
}

/**
 * Check whether a URL was already seen.
 * @param {string} crawlID
 * @param {object} config
 * @param {string} url
 * @param {Callback} callback
 */
function hasSeen(crawlID, config, url, callback) {
  distribution[config.gid].store.get(seenKey(crawlID, url), (error) => {
    if (error) {
      return callback(null, false);
    }
    return callback(null, true);
  });
}

/**
 * Store a crawled document.
 * @param {string} crawlID
 * @param {object} config
 * @param {{url: string, text: string, title: string, depth: number, outlinks: string[]}} doc
 * @param {Callback} callback
 */
function putDoc(crawlID, config, doc, callback) {
  putValue(config.gid, docKey(crawlID, doc.url), doc, callback);
}

/**
 * Read a stored document.
 * @param {string} crawlID
 * @param {object} config
 * @param {string} url
 * @param {Callback} callback
 */
function getDoc(crawlID, config, url, callback) {
  distribution[config.gid].store.get(docKey(crawlID, url), callback);
}

function putValue(gid, key, value, callback) {
  distribution[gid].store.put(value, { key, gid }, callback);
}

function writeManifest(gid, key, value, callback) {
  distribution[gid].store.put(value, { key, gid }, callback);
}

function readManifest(gid, key, callback) {
  distribution[gid].store.get({ key, gid }, (error, value) => {
    if (error) {
      return callback(null, []);
    }
    return callback(null, Array.isArray(value) ? value : []);
  });
}

function settleAll(promises, callback) {
  if (promises.length === 0) {
    return callback(null);
  }

  let remaining = promises.length;
  let firstError = null;
  promises.forEach((promise) => {
    promise((error) => {
      if (error && !firstError) {
        firstError = error;
      }
      remaining--;
      if (remaining === 0) {
        callback(firstError, null);
      }
    });
  });
}

module.exports = {
  init,
  getFrontier,
  setFrontier,
  appendDocs,
  markSeen,
  hasSeen,
  putDoc,
  getDoc,
  frontierKey,
  docKey,
  seenKey,
  manifestKey,
};
