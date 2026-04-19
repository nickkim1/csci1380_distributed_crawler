#!/usr/bin/env node
// @ts-nocheck

const makeDistribution = require("../distribution.js");
const distribution = makeDistribution();
const id = distribution.util.id;
const fs = require("node:fs");
const path = require("node:path");

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

function hasFlag(name) {
  return argv.includes(`--${name}`);
}

function readBoolean(name, fallback = false) {
  if (!hasFlag(name)) {
    return fallback;
  }

  const raw = readFlag(name);
  if (raw === null) {
    return true;
  }
  const normalized = String(raw).toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function printUsage() {
  console.log(
    `Usage:\n  node scripts/m6_pipeline_cli.js --seed <url> --query <text> [options]\n\nOptions:\n  --seed <url>           Seed URL to crawl (required)\n  --query <text>         Query string to execute (required)\n  --gid <name>           Group id (default: cli_pipeline)\n  --indexGid <name>      Index gid (default: index_<gid>)\n  --maxDepth <n>         Crawl max depth (default: 1)\n  --maxPages <n>         Crawl page budget (default: 40)\n  --limit <n>            Query result limit (default: 10)\n  --workerCount <n>      Number of workers to spawn (default: 3)\n  --basePort <n>         First worker port (default: 9061)\n  --cacheFile <path>     Cache file path (default: .cache/m6_pipeline_<gid>.json)\n  --useCache <bool>      Reuse cached index when config matches (default: true)\n  --refreshCache <bool>  Ignore cache and rebuild index (default: false)\n  --help                 Show this help`,
  );
}

function hasError(err) {
  if (!err) {
    return false;
  }
  if (err instanceof Error) {
    return true;
  }
  if (typeof err === "object") {
    return Object.keys(err).length > 0;
  }
  return true;
}

const seed = readFlag("seed");
const query = readFlag("query");
if (!seed || !query || hasFlag("help")) {
  printUsage();
  process.exitCode = hasFlag("help") ? 0 : 1;
  process.exit();
}

const gid = readFlag("gid") || "cli_pipeline";
const indexGid = readFlag("indexGid") || `index_${gid}`;
const maxDepth = Math.max(0, readNumber("maxDepth", 1));
const maxPages = Math.max(1, readNumber("maxPages", 40));
const limit = Math.max(1, readNumber("limit", 10));
const workerCount = Math.max(1, readNumber("workerCount", 3));
const basePort = Math.max(1025, readNumber("basePort", 9061));
const useCache = readBoolean("useCache", true);
const refreshCache = readBoolean("refreshCache", false);
const defaultCacheFile = path.join(
  process.cwd(),
  ".cache",
  `m6_pipeline_${gid}.json`,
);
const cacheFile = readFlag("cacheFile") || defaultCacheFile;

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

function loadCache() {
  if (!useCache || refreshCache || !fs.existsSync(cacheFile)) {
    return null;
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(cacheFile, "utf8"));
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    const matchesConfig =
      parsed.seed === seed &&
      parsed.gid === gid &&
      parsed.indexGid === indexGid &&
      Number(parsed.maxDepth) === maxDepth &&
      Number(parsed.maxPages) === maxPages &&
      Number(parsed.workerCount) === workerCount &&
      Number(parsed.basePort) === basePort &&
      parsed.shardsBySidPacked &&
      typeof parsed.shardsBySidPacked === "object";

    return matchesConfig ? parsed : null;
  } catch (_error) {
    return null;
  }
}

function writeCache(payload) {
  try {
    ensureCacheDir();
    fs.writeFileSync(cacheFile, JSON.stringify(payload), "utf8");
  } catch (_error) {
    // Cache write failure should not break pipeline flow.
  }
}

function hydrateIndexFromCache(cacheData, callback) {
  const shardsBySid = unpackShardsFromCache(cacheData);
  const sidList = Object.keys(shardsBySid);

  if (sidList.length === 0) {
    return callback(Error("cache contains no index shards"));
  }

  let pending = sidList.length;
  let failed = false;

  sidList.forEach((sid) => {
    const node = workers.find((n) => id.getSID(n) === sid);
    if (!node) {
      failed = true;
      return callback(
        Error(
          `cache sid ${sid} not present in current worker set; rebuild needed`,
        ),
      );
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
    return callback(null, {});
  }

  const shardsBySid = {};
  let pending = sidList.length;
  let failed = false;

  sidList.forEach((sid) => {
    const fileKey = files[sid];
    const node = workers.find((n) => id.getSID(n) === sid);
    if (!node) {
      failed = true;
      return callback(
        Error(`cannot snapshot shard for sid ${sid}: worker not found`),
      );
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
    if (hasError(startErr)) {
      return callback(startErr);
    }

    let i = 0;
    const spawnNext = () => {
      if (i >= workers.length) {
        return buildGroup(callback);
      }
      const node = workers[i++];
      distribution.local.status.spawn(node, (spawnErr) => {
        if (hasError(spawnErr)) {
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
    if (hasError(localErr)) {
      return callback(localErr);
    }

    distribution[gid].groups.put({ gid }, group, (groupErr) => {
      if (hasError(groupErr)) {
        return callback(groupErr);
      }
      callback(null);
    });
  });
}

function stopCluster(callback) {
  let i = 0;

  const stopNext = () => {
    if (i >= workers.length) {
      if (distribution.node.server) {
        return distribution.node.server.close(() => callback());
      }
      return callback();
    }

    distribution.local.comm.send(
      [],
      { node: workers[i++], service: "status", method: "stop" },
      () => stopNext(),
    );
  };

  stopNext();
}

function main() {
  const startedAt = Date.now();
  const cacheData = loadCache();

  startCluster((clusterErr) => {
    if (clusterErr) {
      console.error("[m6-pipeline-cli] cluster startup failed:", clusterErr);
      process.exitCode = 1;
      return;
    }

    const runQuery = (pipelineStats, crawlAndIndexMs, usedCache) => {
      const queryStart = Date.now();
      distribution[gid].query.exec(
        {
          indexGid: pipelineStats?.indexGid || indexGid,
          query,
          limit,
        },
        (queryErr, queryOut) => {
          if (queryErr) {
            console.error("[m6-pipeline-cli] query failed:", queryErr);
            return stopCluster(() => {
              process.exitCode = 1;
            });
          }

          const output = {
            seed,
            query,
            gid,
            usedCache,
            cacheFile,
            indexGid: pipelineStats?.indexGid || indexGid,
            config: {
              maxDepth,
              maxPages,
              limit,
              workerCount,
              basePort,
            },
            metrics: {
              docs: Number(pipelineStats?.docs || 0),
              terms: Number(pipelineStats?.terms || 0),
              pagesFetched: Number(
                pipelineStats?.crawlStats?.pagesFetched || 0,
              ),
            },
            timingMs: {
              crawlAndIndex: crawlAndIndexMs,
              query: Date.now() - queryStart,
              total: Date.now() - startedAt,
            },
            queryResult: queryOut || { query, terms: [], results: [] },
          };

          console.log(JSON.stringify(output, null, 2));

          stopCluster(() => {
            process.exitCode = 0;
          });
        },
      );
    };

    const runBuild = () => {
      const crawlStart = Date.now();
      distribution[gid].crawler.exec(
        {
          urls: [seed],
          indexGid,
          maxDepth,
          maxPages,
        },
        (crawlErr, crawlStats) => {
          if (crawlErr) {
            console.error("[m6-pipeline-cli] crawl/index failed:", crawlErr);
            return stopCluster(() => {
              process.exitCode = 1;
            });
          }

          const crawlAndIndexMs = Date.now() - crawlStart;
          snapshotIndexToCache(crawlStats, (snapErr, shardsBySid) => {
            if (!snapErr) {
              writeCache({
                seed,
                gid,
                indexGid: crawlStats.indexGid || indexGid,
                maxDepth,
                maxPages,
                workerCount,
                basePort,
                docs: crawlStats.docs || 0,
                terms: crawlStats.terms || 0,
                pagesFetched: Number(crawlStats?.crawlStats?.pagesFetched || 0),
                files: crawlStats.files || {},
                shardsBySidPacked: packShardsForCache(shardsBySid),
                createdAt: new Date().toISOString(),
              });
            }

            runQuery(crawlStats, crawlAndIndexMs, false);
          });
        },
      );
    };

    if (cacheData) {
      const hydrateStart = Date.now();
      return hydrateIndexFromCache(cacheData, (hydrateErr) => {
        if (hydrateErr) {
          console.error(
            "[m6-pipeline-cli] cache hydrate failed, rebuilding:",
            hydrateErr,
          );
          return runBuild();
        }

        runQuery(
          {
            docs: cacheData.docs || 0,
            terms: cacheData.terms || 0,
            indexGid: cacheData.indexGid || indexGid,
            files: cacheData.files || {},
            crawlStats: {
              pagesFetched: cacheData.pagesFetched || 0,
            },
          },
          Date.now() - hydrateStart,
          true,
        );
      });
    }

    runBuild();
  });
}

main();
