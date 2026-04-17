#!/usr/bin/env node
// @ts-nocheck

/**
 * End-to-end M6 test script for Sandbox 3 large collection.
 *
 * Default seed:
 *   https://cs.brown.edu/courses/csci1380/sandbox/3/
 *
 * Usage examples:
 *   node scripts/m6_e2e_sandbox3_large.js
 *   node scripts/m6_e2e_sandbox3_large.js --maxPages 150 --maxDepth 2
 *   node scripts/m6_e2e_sandbox3_large.js --queries "book summary,science fiction,love quote"
 */

const makeDistribution = require("../distribution.js");
const distribution = makeDistribution();
const id = distribution.util.id;
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const argv = process.argv.slice(2);

function readFlag(name) {
  const i = argv.indexOf(`--${name}`);
  if (i === -1) {
    return null;
  }
  return argv[i + 1] || null;
}

function readNumber(name, fallback) {
  const raw = readFlag(name);
  if (!raw) {
    return fallback;
  }
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

const seed =
  readFlag("seed") ||
  "https://cs.brown.edu/courses/csci1380/sandbox/3/";
const gid = readFlag("gid") || "sandbox3";
const indexGid = readFlag("indexGid") || `index_${gid}`;
const maxDepth = readNumber("maxDepth", 2);
const pageBudget = readNumber("pageBudget", readNumber("maxPages", 1000));
const limit = readNumber("limit", 10);
const workerCount = Math.max(1, readNumber("workerCount", 3));
const basePort = Math.max(1025, readNumber("basePort", 7761));
const refreshCache = readFlag("refreshCache") === "true";
const cacheFile =
  readFlag("cacheFile") ||
  path.join(process.cwd(), ".cache", "m6_sandbox3_index_cache.json");

const queryList =
  (readFlag("queries") || "book summary,english literature,author")
    .split(",")
    .map((q) => q.trim())
    .filter(Boolean);

const workers = Array.from({ length: workerCount }, (_unused, i) => ({
  ip: "127.0.0.1",
  port: basePort + i,
}));

function ensureCacheDir() {
  const dir = path.dirname(cacheFile);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function loadCache() {
  if (refreshCache || !fs.existsSync(cacheFile)) {
    return null;
  }

  try {
    const raw = fs.readFileSync(cacheFile);
    const text = cacheFile.endsWith(".gz")
      ? zlib.gunzipSync(raw).toString("utf8")
      : raw.toString("utf8");
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    if (
      parsed.seed !== seed ||
      parsed.gid !== gid ||
      (!parsed.shardsBySid && !parsed.shardsBySidPacked)
    ) {
      return null;
    }
    return parsed;
  } catch (_error) {
    return null;
  }
}

function packShardsForCache(shardsBySid) {
  const packed = {};
  Object.entries(shardsBySid || {}).forEach(([sid, shard]) => {
    const terms = [];
    Object.entries(shard || {}).forEach(([term, postings]) => {
      const rows = Array.isArray(postings)
        ? postings
            .filter((row) => row && typeof row.url === "string")
            .map((row) => [row.url, Number(row.count || 0)])
        : [];
      if (rows.length > 0) {
        terms.push([term, rows]);
      }
    });
    packed[sid] = terms;
  });
  return packed;
}

function unpackShardsFromCache(cacheData) {
  if (cacheData?.shardsBySid && typeof cacheData.shardsBySid === "object") {
    return cacheData.shardsBySid;
  }

  const packed = cacheData?.shardsBySidPacked;
  if (!packed || typeof packed !== "object") {
    return {};
  }

  const unpacked = {};
  Object.entries(packed).forEach(([sid, terms]) => {
    const shard = {};
    (Array.isArray(terms) ? terms : []).forEach((entry) => {
      const term = Array.isArray(entry) ? entry[0] : null;
      const rows = Array.isArray(entry) ? entry[1] : null;
      if (typeof term !== "string") {
        return;
      }
      shard[term] = (Array.isArray(rows) ? rows : [])
        .filter((row) => Array.isArray(row) && typeof row[0] === "string")
        .map((row) => ({ url: row[0], count: Number(row[1] || 0) }));
    });
    unpacked[sid] = shard;
  });
  return unpacked;
}

function writeCache(payload) {
  try {
    ensureCacheDir();
    const compact = JSON.stringify(payload);
    if (cacheFile.endsWith(".gz")) {
      fs.writeFileSync(cacheFile, zlib.gzipSync(Buffer.from(compact, "utf8")));
    } else {
      fs.writeFileSync(cacheFile, compact, "utf8");
    }
  } catch (_error) {
    // Cache write failure should not break e2e flow.
  }
}

function hydrateIndexFromCache(cacheData, callback) {
  const shardsBySid = unpackShardsFromCache(cacheData);
  const sidList = Object.keys(shardsBySid);

  if (sidList.length === 0) {
    return callback(null);
  }

  let pending = sidList.length;
  let failed = false;

  sidList.forEach((sid) => {
    const node = workers.find((n) => id.getSID(n) === sid);
    if (!node) {
      pending--;
      if (pending === 0 && !failed) {
        callback(null);
      }
      return;
    }

    const fileKey = `inv_${cacheData.indexGid}_${sid}`;
    distribution.local.comm.send(
      [shardsBySid[sid], { key: fileKey, gid: cacheData.indexGid }],
      { node, service: "store", method: "put", gid: "local" },
      (putErr) => {
        if (failed) {
          return;
        }
        if (putErr) {
          failed = true;
          return callback(putErr);
        }
        pending--;
        if (pending === 0) {
          callback(null);
        }
      },
    );
  });
}

function snapshotIndexToCache(crawlStats, callback) {
  const files = crawlStats?.files || {};
  const sidList = Object.keys(files);

  if (sidList.length === 0) {
    return callback(null, null);
  }

  const shardsBySid = {};
  let pending = sidList.length;
  let failed = false;

  sidList.forEach((sid) => {
    const fileKey = files[sid];
    const node = workers.find((n) => id.getSID(n) === sid);
    if (!node) {
      pending--;
      if (pending === 0 && !failed) {
        callback(null, shardsBySid);
      }
      return;
    }

    distribution.local.comm.send(
      [{ key: fileKey, gid: crawlStats.indexGid }],
      { node, service: "store", method: "get", gid: "local" },
      (getErr, shard) => {
        if (failed) {
          return;
        }
        if (getErr) {
          failed = true;
          return callback(getErr, null);
        }
        shardsBySid[sid] = shard || {};
        pending--;
        if (pending === 0) {
          callback(null, shardsBySid);
        }
      },
    );
  });
}

function startCluster(callback) {
  distribution.node.start((startErr) => {
    if (startErr) {
      return callback(startErr);
    }

    let idx = 0;
    const spawnNext = () => {
      if (idx >= workers.length) {
        return buildGroup(callback);
      }
      const node = workers[idx++];
      distribution.local.status.spawn(node, (spawnErr) => {
        if (spawnErr) {
          return callback(spawnErr);
        }
        spawnNext();
      });
    };

    spawnNext();
  });
}

function buildGroup(callback) {
  const group = {};
  workers.forEach((node) => {
    group[id.getSID(node)] = node;
  });

  distribution.local.groups.put({ gid }, group, (localErr) => {
    if (localErr) {
      return callback(localErr);
    }

    distribution[gid].groups.put({ gid }, group, (groupErr) => {
      if (groupErr && Object.keys(groupErr).length) {
        return callback(new Error(`group put failed: ${JSON.stringify(groupErr)}`));
      }
      callback(null);
    });
  });
}

function stopCluster(callback) {
  let idx = 0;

  const stopNext = () => {
    if (idx >= workers.length) {
      if (distribution.node.server) {
        distribution.node.server.close();
      }
      return callback();
    }

    const node = workers[idx++];
    distribution.local.comm.send(
      [],
      { node, service: "status", method: "stop" },
      () => stopNext(),
    );
  };

  stopNext();
}

function runQueries(indexName, callback) {
  const results = [];
  let i = 0;

  const runNext = () => {
    if (i >= queryList.length) {
      return callback(null, results);
    }

    const queryText = queryList[i++];
    const qStart = Date.now();

    distribution[gid].query.exec(
      {
        indexGid: indexName,
        query: queryText,
        limit,
      },
      (qErr, qRes) => {
        if (qErr) {
          return callback(qErr);
        }

        results.push({
          query: queryText,
          latencyMs: Date.now() - qStart,
          resultCount: Array.isArray(qRes?.results) ? qRes.results.length : 0,
          topResults: Array.isArray(qRes?.results)
            ? qRes.results.slice(0, limit)
            : [],
        });

        runNext();
      },
    );
  };

  runNext();
}

function main() {
  const startedAt = Date.now();
  const cacheData = loadCache();

  startCluster((clusterErr) => {
    if (clusterErr) {
      console.error("[m6-sandbox3] cluster startup failed:", clusterErr);
      process.exitCode = 1;
      return;
    }

    const runWithQuery = (pipelineStats, crawlAndIndexMs, usedCache) => {
      runQueries(pipelineStats?.indexGid || indexGid, (queryErr, queryReports) => {
          if (queryErr) {
            console.error("[m6-sandbox3] query phase failed:", queryErr);
            return stopCluster(() => {
              process.exitCode = 1;
            });
          }

          const phase = pipelineStats?.crawlStats || {};

          const output = {
            corpus: "sandbox3-large-books",
            seed,
            gid,
            indexGid: pipelineStats?.indexGid || indexGid,
            usedCache,
            cacheFile,
            config: {
              maxDepth,
              pageBudget,
              resultLimit: limit,
              workerCount: workers.length,
            },
            metrics: {
              pagesFetched: Number(phase.pagesFetched || 0),
              booksCrawled: Number(phase.bookDocs || pipelineStats?.docs || 0),
              booksIndexed: Number(pipelineStats?.docs || 0),
              indexedTerms: Number(pipelineStats?.terms || 0),
            },
            timingMs: {
              crawl: Number(phase.crawlMs || 0),
              index: Number(phase.indexMs || 0),
              crawlAndIndex: crawlAndIndexMs,
              total: Date.now() - startedAt,
            },
            queryReports,
          };

          console.log(
            `[crawler] books in index: ${output.metrics.booksIndexed}, pages traversed: ${output.metrics.pagesFetched}`,
          );

          console.log(JSON.stringify(output, null, 2));

          stopCluster(() => {
            process.exitCode = 0;
          });
        });
    };

    if (cacheData) {
      const hydrateStart = Date.now();
      return hydrateIndexFromCache(cacheData, (hydrateErr) => {
        if (hydrateErr) {
          console.error("[m6-sandbox3] cache hydrate failed, rebuilding:", hydrateErr);
          return runBuild();
        }

        runWithQuery(
          {
            docs: cacheData.docs || 0,
            terms: cacheData.terms || 0,
            indexGid: cacheData.indexGid,
            files: cacheData.files || {},
            crawlStats: {
              seeds: 1,
              fallbackUsed: true,
              cacheRestored: true,
              pagesFetched: cacheData.pagesFetched || 0,
              bookDocs: cacheData.docs || 0,
              crawlMs: 0,
              indexMs: 0,
            },
          },
          Date.now() - hydrateStart,
          true,
        );
      });
    }

    runBuild();

    function runBuild() {
      const crawlStart = Date.now();
      distribution[gid].crawler.exec(
        {
          urls: [seed],
          indexGid,
          maxDepth,
          maxPages: pageBudget,
        },
        (crawlErr, crawlStats) => {
          if (crawlErr) {
            console.error("[m6-sandbox3] crawl+index failed:", crawlErr);
            return stopCluster(() => {
              process.exitCode = 1;
            });
          }

          const crawlAndIndexMs = Date.now() - crawlStart;

          snapshotIndexToCache(crawlStats, (snapErr, shardsBySid) => {
            if (!snapErr && shardsBySid) {
              writeCache({
                seed,
                gid,
                indexGid: crawlStats.indexGid || indexGid,
                docs: crawlStats.docs || 0,
                terms: crawlStats.terms || 0,
                files: crawlStats.files || {},
                pagesFetched: Number(crawlStats?.crawlStats?.pagesFetched || 0),
                shardsBySidPacked: packShardsForCache(shardsBySid),
              });
            }

            runWithQuery(crawlStats, crawlAndIndexMs, false);
          });
        },
      );
    }
  });
}

main();
