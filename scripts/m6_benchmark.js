#!/usr/bin/env node
// @ts-nocheck

const fs = require("node:fs");
const path = require("node:path");
const { performance } = require("node:perf_hooks");
const makeDistribution = require("../distribution.js");

const distribution = makeDistribution();
const id = distribution.util.id;

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

function printUsage() {
  console.log(
    `Usage:\n  node scripts/m6_benchmark.js --seed <url> [options]\n\nOptions:\n  --seed <url>             Seed URL for crawl/index (required)\n  --gid <name>             Group id (default: bench_pipeline)\n  --indexGid <name>        Index gid (default: index_<gid>)\n  --queries <csv>          Query workload (default: book summary,mystery,history)\n  --maxDepth <n>           Crawl max depth (default: 1)\n  --maxPages <n>           Crawl page budget (default: 30)\n  --workerCount <n>        Number of workers to spawn (default: 3)\n  --basePort <n>           First worker port (default: 9361)\n  --nodes <csv>            Explicit nodes as ip:port,ip:port (overrides local worker list)\n  --nodesFile <path>       JSON file with nodes array [{ip,port}, ...]\n  --spawnWorkers <bool>    Spawn workers from this coordinator (default: true local, false with --nodes/--nodesFile)\n  --stopWorkers <bool>     Stop workers on teardown (default: same as spawnWorkers)\n  --componentTimeoutMs <n> Timeout budget per measured component (default: 120000)\n  --queryRuns <n>          Number of query runs for search benchmark (default: 20)\n  --storageOps <n>         Put/Get operations for storage benchmark (default: 50)\n  --rankingRuns <n>        Number of ranking runs (default: 80)\n  --resultsDir <path>      Base directory for output artifacts (default: results/perf)\n  --cacheFile <path>       Cache file path (default: .cache/m6_benchmark_<gid>.json)\n  --useCache <bool>        Reuse cached index when config matches (default: true)\n  --refreshCache <bool>    Ignore cache and rebuild index (default: false)\n  --help                   Show this help`,
  );
}

function parseNodesCsv(raw) {
  if (!raw) {
    return [];
  }
  return raw
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const [ip, portRaw] = part.split(":");
      const port = Number(portRaw);
      if (!ip || !Number.isFinite(port)) {
        throw new Error(`invalid node entry: ${part}`);
      }
      return { ip, port };
    });
}

function readNodesFromFile(filePath) {
  if (!filePath) {
    return [];
  }

  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
  if (!Array.isArray(parsed)) {
    throw new Error("nodesFile JSON must be an array of {ip, port}");
  }

  return parsed.map((node) => {
    const ip = node && node.ip;
    const port = Number(node && node.port);
    if (typeof ip !== "string" || !Number.isFinite(port)) {
      throw new Error(`invalid node in nodesFile: ${JSON.stringify(node)}`);
    }
    return { ip, port };
  });
}

const seed = readFlag("seed");
if (!seed || hasFlag("help")) {
  printUsage();
  process.exitCode = hasFlag("help") ? 0 : 1;
  process.exit();
}

const gid = readFlag("gid") || "bench_pipeline";
const indexGid = readFlag("indexGid") || `index_${gid}`;
const maxDepth = Math.max(0, readNumber("maxDepth", 1));
const maxPages = Math.max(1, readNumber("maxPages", 30));
const workerCount = Math.max(1, readNumber("workerCount", 3));
const basePort = Math.max(1025, readNumber("basePort", 9361));
const componentTimeoutMs = Math.max(1000, readNumber("componentTimeoutMs", 120000));
const queryRuns = Math.max(1, readNumber("queryRuns", 20));
const storageOps = Math.max(2, readNumber("storageOps", 50));
const rankingRuns = Math.max(1, readNumber("rankingRuns", 80));
const resultsBaseDir =
  readFlag("resultsDir") || path.join(process.cwd(), "results", "perf");
const useCache = readBoolean("useCache", true);
const refreshCache = readBoolean("refreshCache", false);
const defaultCacheFile = path.join(
  process.cwd(),
  ".cache",
  `m6_benchmark_${gid}.json`,
);
const cacheFile = readFlag("cacheFile") || defaultCacheFile;
const queryList = (readFlag("queries") || "book summary,mystery,history")
  .split(",")
  .map((q) => q.trim())
  .filter(Boolean);

let explicitNodes = [];
try {
  explicitNodes = [
    ...readNodesFromFile(readFlag("nodesFile")),
    ...parseNodesCsv(readFlag("nodes")),
  ];
} catch (nodeParseErr) {
  console.error("[m6-benchmark] failed to parse node list:", nodeParseErr);
  process.exit(1);
}

const hasExplicitNodes = explicitNodes.length > 0;
const spawnWorkers = readBoolean("spawnWorkers", !hasExplicitNodes);
const stopWorkers = readBoolean("stopWorkers", spawnWorkers);

const workers = hasExplicitNodes
  ? explicitNodes
  : Array.from({ length: workerCount }, (_unused, i) => ({
      ip: "127.0.0.1",
      port: basePort + i,
    }));

function ensureDirectory(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function timestamp() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}

function ensureCacheDir() {
  ensureDirectory(path.dirname(cacheFile));
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
      JSON.stringify(parsed.workers || []) === JSON.stringify(workers) &&
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
    // Cache write failure should not break benchmark flow.
  }
}

function startCluster(callback) {
  distribution.node.start((startErr) => {
    if (hasError(startErr)) {
      return callback(startErr);
    }

    if (!spawnWorkers) {
      return buildGroup(callback);
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
  if (!stopWorkers) {
    if (distribution.node.server) {
      return distribution.node.server.close(() => callback());
    }
    return callback();
  }

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

function snapshotIndex(crawlStats, callback) {
  const files = crawlStats?.files || {};
  const sidList = Object.keys(files);

  if (sidList.length === 0) {
    return callback(null, {});
  }

  const shardsBySid = {};
  let pending = sidList.length;
  let failed = false;

  sidList.forEach((sid) => {
    const node = workers.find((n) => id.getSID(n) === sid);
    const fileKey = files[sid];

    if (!node) {
      failed = true;
      return callback(
        Error(`worker for sid ${sid} not found while snapshotting index`),
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

function benchmarkStorage(callback) {
  const deadline = performance.now() + componentTimeoutMs;
  const putLatencies = [];
  const getLatencies = [];
  const keys = Array.from(
    { length: storageOps },
    (_unused, i) => `bench_store_${Date.now()}_${i}`,
  );

  const componentStart = performance.now();
  const putStart = performance.now();
  let i = 0;

  const finish = (putTotalMs, getTotalMs, timedOut) => {
    const totalOps = putLatencies.length + getLatencies.length;
    const totalMs = performance.now() - componentStart;
    return callback(null, {
      putLatencyAvgMs: average(putLatencies),
      getLatencyAvgMs: average(getLatencies),
      latencyMs: (average(putLatencies) + average(getLatencies)) / 2,
      throughputPerSec: totalMs > 0 ? (totalOps * 1000) / totalMs : 0,
      samples: totalOps,
      unit: "ops/s",
      timedOut: Boolean(timedOut),
      putTotalMs,
      getTotalMs,
    });
  };

  const runPut = () => {
    if (performance.now() >= deadline) {
      return finish(performance.now() - putStart, 0, true);
    }

    if (i >= keys.length) {
      const putTotalMs = performance.now() - putStart;
      const getStart = performance.now();
      let j = 0;

      const runGet = () => {
        if (performance.now() >= deadline) {
          return finish(putTotalMs, performance.now() - getStart, true);
        }

        if (j >= keys.length) {
          const getTotalMs = performance.now() - getStart;
          return finish(putTotalMs, getTotalMs, false);
        }

        const t0 = performance.now();
        distribution[gid].store.get(keys[j], (getErr) => {
          if (hasError(getErr)) {
            return callback(getErr);
          }
          getLatencies.push(performance.now() - t0);
          j++;
          runGet();
        });
      };

      return runGet();
    }

    const value = {
      id: i,
      text: `storage_payload_${i}`,
      nested: { score: i % 7 },
    };

    const t0 = performance.now();
    distribution[gid].store.put(value, keys[i], (putErr) => {
      if (hasError(putErr)) {
        return callback(putErr);
      }
      putLatencies.push(performance.now() - t0);
      i++;
      runPut();
    });
  };

  runPut();
}

function benchmarkSearch(indexName, callback) {
  const deadline = performance.now() + componentTimeoutMs;
  const latencies = [];
  const start = performance.now();
  let i = 0;

  const runNext = () => {
    if (performance.now() >= deadline) {
      const totalMs = performance.now() - start;
      return callback(null, {
        latencyMs: average(latencies),
        throughputPerSec: totalMs > 0 ? (latencies.length * 1000) / totalMs : 0,
        samples: latencies.length,
        unit: "queries/s",
        timedOut: true,
      });
    }

    if (i >= queryRuns) {
      const totalMs = performance.now() - start;
      return callback(null, {
        latencyMs: average(latencies),
        throughputPerSec: totalMs > 0 ? (queryRuns * 1000) / totalMs : 0,
        samples: queryRuns,
        unit: "queries/s",
        timedOut: false,
      });
    }

    const q = queryList[i % queryList.length];
    const t0 = performance.now();
    distribution[gid].query.exec(
      {
        indexGid: indexName,
        query: q,
        limit: 10,
      },
      (queryErr, queryOut) => {
        if (hasError(queryErr)) {
          return callback(queryErr);
        }
        if (!queryOut || !Array.isArray(queryOut.results)) {
          return callback(Error("query benchmark expected results array"));
        }

        latencies.push(performance.now() - t0);
        i++;
        runNext();
      },
    );
  };

  runNext();
}

function benchmarkRanking(shardsBySid, callback) {
  const terms = [];
  Object.values(shardsBySid || {}).forEach((shard) => {
    Object.keys(shard || {}).forEach((term) => {
      if (Array.isArray(shard[term]) && shard[term].length > 0) {
        terms.push(term);
      }
    });
  });

  const uniqueTerms = Array.from(new Set(terms));
  if (uniqueTerms.length === 0) {
    return callback(null, {
      latencyMs: 0,
      throughputPerSec: 0,
      samples: 0,
      unit: "rankings/s",
      candidateUrlsPerRun: 0,
    });
  }

  const latencies = [];
  let accumulatedCandidates = 0;
  const start = performance.now();
  const deadline = start + componentTimeoutMs;

  let i = 0;
  for (; i < rankingRuns; i++) {
    if (performance.now() >= deadline) {
      break;
    }

    const t0 = performance.now();
    const termA = uniqueTerms[i % uniqueTerms.length];
    const termB = uniqueTerms[(i + 1) % uniqueTerms.length];
    const termC = uniqueTerms[(i + 2) % uniqueTerms.length];

    const postings = [];
    Object.values(shardsBySid).forEach((shard) => {
      [termA, termB, termC].forEach((term) => {
        const rows = Array.isArray(shard[term]) ? shard[term] : [];
        rows.forEach((row) => postings.push(row));
      });
    });

    const ranked = rankRows(postings);
    accumulatedCandidates += ranked.length;
    latencies.push(performance.now() - t0);
  }

  const totalMs = performance.now() - start;
  callback(null, {
    latencyMs: average(latencies),
    throughputPerSec: totalMs > 0 ? (i * 1000) / totalMs : 0,
    samples: i,
    unit: "rankings/s",
    candidateUrlsPerRun:
      i > 0 ? accumulatedCandidates / i : 0,
    timedOut: i < rankingRuns,
  });
}

function rankRows(rows) {
  const scores = new Map();
  rows.forEach((row) => {
    if (!row || typeof row.url !== "string") {
      return;
    }
    scores.set(row.url, (scores.get(row.url) || 0) + Number(row.count || 0));
  });

  return Array.from(scores.entries())
    .map(([url, score]) => ({ url, score }))
    .sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
}

function average(values) {
  if (!values || values.length === 0) {
    return 0;
  }
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function round(value) {
  return Number((Number(value) || 0).toFixed(4));
}

function renderBarChartSvg(title, yLabel, entries, valueKey) {
  const width = 980;
  const height = 520;
  const padLeft = 90;
  const padRight = 20;
  const padTop = 70;
  const padBottom = 90;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;

  const values = entries.map((e) => Number(e[valueKey] || 0));
  const max = Math.max(...values, 1);
  const barGap = 16;
  const barWidth = Math.max(
    30,
    (plotWidth - barGap * (entries.length + 1)) / Math.max(entries.length, 1),
  );

  const bars = entries
    .map((entry, idx) => {
      const value = Number(entry[valueKey] || 0);
      const scaled = (value / max) * plotHeight;
      const x = padLeft + barGap + idx * (barWidth + barGap);
      const y = padTop + (plotHeight - scaled);
      const labelX = x + barWidth / 2;
      const display = round(value);
      return `<rect x="${x}" y="${y}" width="${barWidth}" height="${scaled}" fill="#f58518" />\n<text x="${labelX}" y="${y - 8}" text-anchor="middle" font-size="12" fill="#222">${display}</text>\n<text x="${labelX}" y="${padTop + plotHeight + 22}" text-anchor="middle" font-size="13" fill="#222">${entry.name}</text>`;
    })
    .join("\n");

  const yTicks = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const value = round(max * ratio);
      const y = padTop + plotHeight - ratio * plotHeight;
      return `<line x1="${padLeft}" y1="${y}" x2="${padLeft + plotWidth}" y2="${y}" stroke="#ddd" />\n<text x="${padLeft - 10}" y="${y + 4}" text-anchor="end" font-size="12" fill="#333">${value}</text>`;
    })
    .join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="#fff" />
  <text x="${width / 2}" y="32" text-anchor="middle" font-size="24" font-family="Helvetica, Arial, sans-serif" fill="#111">${title}</text>
  ${yTicks}
  <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${padTop + plotHeight}" stroke="#444" />
  <line x1="${padLeft}" y1="${padTop + plotHeight}" x2="${padLeft + plotWidth}" y2="${padTop + plotHeight}" stroke="#444" />
  ${bars}
  <text x="${padLeft - 60}" y="${padTop + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 ${padLeft - 60} ${padTop + plotHeight / 2})" font-size="13" fill="#222">${yLabel}</text>
</svg>`;
}

function writeArtifacts(resultDir, payload) {
  ensureDirectory(resultDir);

  const latencyEntries = payload.components.map((c) => ({
    name: c.name,
    latencyMs: c.latencyMs,
  }));
  const throughputEntries = payload.components.map((c) => ({
    name: c.name,
    throughputPerSec: c.throughputPerSec,
  }));

  const latencySvg = renderBarChartSvg(
    "Component Latency",
    "Latency (ms)",
    latencyEntries,
    "latencyMs",
  );
  const throughputSvg = renderBarChartSvg(
    "Component Throughput",
    "Throughput (/sec)",
    throughputEntries,
    "throughputPerSec",
  );

  const summaryPath = path.join(resultDir, "summary.json");
  const csvPath = path.join(resultDir, "metrics.csv");
  const latencyPath = path.join(resultDir, "latency_bar.svg");
  const throughputPath = path.join(resultDir, "throughput_bar.svg");
  const htmlPath = path.join(resultDir, "index.html");

  fs.writeFileSync(summaryPath, JSON.stringify(payload, null, 2), "utf8");

  const csvLines = [
    "component,latency_ms,throughput_per_sec,samples,unit",
    ...payload.components.map(
      (c) =>
        `${c.name},${round(c.latencyMs)},${round(c.throughputPerSec)},${c.samples || 0},${c.unit || ""}`,
    ),
  ];
  fs.writeFileSync(csvPath, `${csvLines.join("\n")}\n`, "utf8");
  fs.writeFileSync(latencyPath, latencySvg, "utf8");
  fs.writeFileSync(throughputPath, throughputSvg, "utf8");

  const rows = payload.components
    .map(
      (c) =>
        `<tr><td>${c.name}</td><td>${round(c.latencyMs)}</td><td>${round(c.throughputPerSec)}</td><td>${c.samples || 0}</td><td>${c.unit || ""}</td></tr>`,
    )
    .join("\n");

  const html = `<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>M6 Benchmark Results</title>
<style>
body { font-family: Helvetica, Arial, sans-serif; margin: 20px; color: #111; }
h1 { margin-bottom: 4px; }
.meta { color: #555; margin-bottom: 18px; }
img { width: 100%; max-width: 980px; border: 1px solid #ddd; margin-bottom: 16px; }
table { border-collapse: collapse; min-width: 760px; }
th, td { border: 1px solid #ddd; padding: 8px 10px; text-align: left; }
th { background: #f8f8f8; }
</style>
</head>
<body>
  <h1>M6 Component Performance Results</h1>
  <div class="meta">Generated: ${payload.generatedAt} | Result dir: ${payload.resultDir}</div>
  <h2>Latency</h2>
  <img src="latency_bar.svg" alt="Latency chart" />
  <h2>Throughput</h2>
  <img src="throughput_bar.svg" alt="Throughput chart" />
  <h2>Metrics Table</h2>
  <table>
    <thead><tr><th>Component</th><th>Latency (ms)</th><th>Throughput (/sec)</th><th>Samples</th><th>Unit</th></tr></thead>
    <tbody>
      ${rows}
    </tbody>
  </table>
</body>
</html>`;

  fs.writeFileSync(htmlPath, html, "utf8");

  return {
    summaryPath,
    csvPath,
    latencyPath,
    throughputPath,
    htmlPath,
  };
}

function runBenchmarkSuite(done) {
  const startedAt = Date.now();
  const cacheData = loadCache();
  const resultDir = path.join(
    resultsBaseDir,
    `${gid}_${timestamp()}`,
  );
  ensureDirectory(resultDir);

  const checkpointPath = path.join(resultDir, "checkpoint_summary.json");
  const checkpointPayload = {
    generatedAt: new Date().toISOString(),
    resultDir,
    seed,
    gid,
    indexGid,
    partial: true,
    components: [
      { name: "crawler", latencyMs: 0, throughputPerSec: 0, samples: 0, unit: "pages/s", timedOut: true },
      { name: "indexer", latencyMs: 0, throughputPerSec: 0, samples: 0, unit: "docs/s", timedOut: true },
      { name: "storage", latencyMs: 0, throughputPerSec: 0, samples: 0, unit: "ops/s", timedOut: true },
      { name: "search", latencyMs: 0, throughputPerSec: 0, samples: 0, unit: "queries/s", timedOut: true },
      { name: "ranking", latencyMs: 0, throughputPerSec: 0, samples: 0, unit: "rankings/s", timedOut: true },
    ],
    crawlIndexSummary: {
      docs: 0,
      terms: 0,
      pagesFetched: 0,
      crawlMs: 0,
      indexMs: 0,
      indexReadyMs: 0,
    },
    details: {
      rankingCandidateUrlsPerRun: 0,
      endToEndMs: 0,
    },
  };

  const writeCheckpoint = () => {
    try {
      checkpointPayload.generatedAt = new Date().toISOString();
      checkpointPayload.details.endToEndMs = Date.now() - startedAt;
      fs.writeFileSync(
        checkpointPath,
        JSON.stringify(checkpointPayload, null, 2),
        "utf8",
      );
    } catch (_error) {
      // best effort only
    }
  };

  const setComponent = (componentName, patch) => {
    const idx = checkpointPayload.components.findIndex(
      (c) => c.name === componentName,
    );
    if (idx === -1) {
      checkpointPayload.components.push({ name: componentName, ...patch });
    } else {
      checkpointPayload.components[idx] = {
        ...checkpointPayload.components[idx],
        ...patch,
      };
    }
    writeCheckpoint();
  };

  writeCheckpoint();

  startCluster((clusterErr) => {
    if (clusterErr) {
      return done(clusterErr);
    }

    const afterIndexReady = (
      crawlStats,
      usedCache,
      indexReadyMs,
      shardsBySid,
    ) => {
      const pagesFetched = Number(crawlStats?.crawlStats?.pagesFetched || 0);
      const docs = Number(crawlStats?.docs || 0);
      const terms = Number(crawlStats?.terms || 0);
      const crawlMs = Number(crawlStats?.crawlStats?.crawlMs || 0);
      const indexMs = Number(crawlStats?.crawlStats?.indexMs || 0);
      const crawlTimedOut = Boolean(crawlStats?.crawlStats?.crawlTimedOut);

      checkpointPayload.crawlIndexSummary = {
        docs,
        terms,
        pagesFetched,
        crawlMs,
        indexMs,
        indexReadyMs,
      };
      setComponent("crawler", {
        latencyMs: pagesFetched > 0 ? crawlMs / pagesFetched : 0,
        throughputPerSec: crawlMs > 0 ? (pagesFetched * 1000) / crawlMs : 0,
        samples: pagesFetched,
        unit: "pages/s",
        timedOut: crawlTimedOut,
      });
      setComponent("indexer", {
        latencyMs: docs > 0 ? indexMs / docs : 0,
        throughputPerSec: indexMs > 0 ? (docs * 1000) / indexMs : 0,
        samples: docs,
        unit: "docs/s",
        timedOut: false,
      });

      benchmarkStorage((storageErr, storageStats) => {
        if (storageErr) {
          return stopCluster(() => done(storageErr));
        }

        setComponent("storage", {
          latencyMs: storageStats.latencyMs,
          throughputPerSec: storageStats.throughputPerSec,
          samples: storageStats.samples,
          unit: storageStats.unit,
          timedOut: Boolean(storageStats.timedOut),
        });

        benchmarkSearch(
          crawlStats.indexGid || indexGid,
          (searchErr, searchStats) => {
            if (searchErr) {
              return stopCluster(() => done(searchErr));
            }

            setComponent("search", {
              latencyMs: searchStats.latencyMs,
              throughputPerSec: searchStats.throughputPerSec,
              samples: searchStats.samples,
              unit: searchStats.unit,
              timedOut: Boolean(searchStats.timedOut),
            });

            benchmarkRanking(shardsBySid, (rankingErr, rankingStats) => {
              if (rankingErr) {
                return stopCluster(() => done(rankingErr));
              }

              setComponent("ranking", {
                latencyMs: rankingStats.latencyMs,
                throughputPerSec: rankingStats.throughputPerSec,
                samples: rankingStats.samples,
                unit: rankingStats.unit,
                timedOut: Boolean(rankingStats.timedOut),
              });

              const components = [
                {
                  name: "crawler",
                  latencyMs: pagesFetched > 0 ? crawlMs / pagesFetched : 0,
                  throughputPerSec:
                    crawlMs > 0 ? (pagesFetched * 1000) / crawlMs : 0,
                  samples: pagesFetched,
                  unit: "pages/s",
                  timedOut: crawlTimedOut,
                },
                {
                  name: "indexer",
                  latencyMs: docs > 0 ? indexMs / docs : 0,
                  throughputPerSec: indexMs > 0 ? (docs * 1000) / indexMs : 0,
                  samples: docs,
                  unit: "docs/s",
                  timedOut: false,
                },
                {
                  name: "storage",
                  latencyMs: storageStats.latencyMs,
                  throughputPerSec: storageStats.throughputPerSec,
                  samples: storageStats.samples,
                  unit: storageStats.unit,
                  timedOut: Boolean(storageStats.timedOut),
                },
                {
                  name: "ranking",
                  latencyMs: rankingStats.latencyMs,
                  throughputPerSec: rankingStats.throughputPerSec,
                  samples: rankingStats.samples,
                  unit: rankingStats.unit,
                  timedOut: Boolean(rankingStats.timedOut),
                },
                {
                  name: "search",
                  latencyMs: searchStats.latencyMs,
                  throughputPerSec: searchStats.throughputPerSec,
                  samples: searchStats.samples,
                  unit: searchStats.unit,
                  timedOut: Boolean(searchStats.timedOut),
                },
              ];

              const payload = {
                generatedAt: new Date().toISOString(),
                resultDir,
                seed,
                gid,
                indexGid: crawlStats.indexGid || indexGid,
                usedCache,
                cacheFile,
                config: {
                  maxDepth,
                  maxPages,
                  workerCount,
                  basePort,
                  workers,
                  spawnWorkers,
                  stopWorkers,
                  componentTimeoutMs,
                  queryRuns,
                  storageOps,
                  rankingRuns,
                  queryList,
                },
                crawlIndexSummary: {
                  docs,
                  terms,
                  pagesFetched,
                  crawlMs,
                  indexMs,
                  indexReadyMs,
                },
                components,
                details: {
                  rankingCandidateUrlsPerRun:
                    rankingStats.candidateUrlsPerRun || 0,
                  endToEndMs: Date.now() - startedAt,
                },
              };

              checkpointPayload.partial = false;
              checkpointPayload.components = components;
              checkpointPayload.crawlIndexSummary = payload.crawlIndexSummary;
              checkpointPayload.details = payload.details;
              writeCheckpoint();

              const artifactPaths = writeArtifacts(resultDir, payload);
              stopCluster(() => done(null, { payload, artifactPaths }));
            });
          },
        );
      });
    };

    const buildIndex = () => {
      const crawlStart = performance.now();
      distribution[gid].crawler.exec(
        {
          urls: [seed],
          indexGid,
          maxDepth,
          maxPages,
          crawlTimeoutMs: componentTimeoutMs,
        },
        (crawlErr, crawlStats) => {
          if (hasError(crawlErr)) {
            return stopCluster(() => done(crawlErr));
          }

          snapshotIndex(crawlStats, (snapErr, shardsBySid) => {
            if (!snapErr) {
              writeCache({
                seed,
                gid,
                indexGid: crawlStats.indexGid || indexGid,
                maxDepth,
                maxPages,
                workerCount,
                basePort,
                workers,
                spawnWorkers,
                stopWorkers,
                componentTimeoutMs,
                docs: crawlStats.docs || 0,
                terms: crawlStats.terms || 0,
                pagesFetched: Number(crawlStats?.crawlStats?.pagesFetched || 0),
                files: crawlStats.files || {},
                shardsBySidPacked: packShardsForCache(shardsBySid),
                createdAt: new Date().toISOString(),
              });
            }

            afterIndexReady(
              crawlStats,
              false,
              performance.now() - crawlStart,
              shardsBySid || {},
            );
          });
        },
      );
    };

    if (cacheData) {
      const hydrateStart = performance.now();
      return hydrateIndexFromCache(cacheData, (hydrateErr) => {
        if (hydrateErr) {
          return buildIndex();
        }

        const cachedStats = {
          docs: cacheData.docs || 0,
          terms: cacheData.terms || 0,
          indexGid: cacheData.indexGid || indexGid,
          files: cacheData.files || {},
          crawlStats: {
            pagesFetched: cacheData.pagesFetched || 0,
            crawlMs: 0,
            indexMs: 0,
          },
        };

        const cachedShards = unpackShardsFromCache(cacheData);
        afterIndexReady(
          cachedStats,
          true,
          performance.now() - hydrateStart,
          cachedShards,
        );
      });
    }

    buildIndex();
  });
}

runBenchmarkSuite((err, out) => {
  if (err) {
    console.error("[m6-benchmark] failed:", err);
    process.exitCode = 1;
    return;
  }

  const { payload, artifactPaths } = out;
  console.log(
    JSON.stringify(
      {
        summary: {
          usedCache: payload.usedCache,
          seed: payload.seed,
          gid: payload.gid,
          components: payload.components.map((c) => ({
            name: c.name,
            latencyMs: round(c.latencyMs),
            throughputPerSec: round(c.throughputPerSec),
          })),
        },
        artifacts: artifactPaths,
      },
      null,
      2,
    ),
  );
  process.exitCode = 0;
});
