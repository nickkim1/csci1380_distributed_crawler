#!/usr/bin/env node
// @ts-nocheck

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

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

function hasExplicitFlag(name) {
  return argv.indexOf(`--${name}`) !== -1;
}

function timestamp() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}

function round(n) {
  return Number((Number(n) || 0).toFixed(4));
}

function average(values) {
  if (!values || values.length === 0) {
    return 0;
  }
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function stddev(values) {
  if (!values || values.length <= 1) {
    return 0;
  }
  const avg = average(values);
  const variance =
    values.reduce((acc, value) => acc + (value - avg) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function ensureDirectory(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function printUsage() {
  console.log(`Usage:
  node scripts/m6_scaling_sweep.js [options]

Purpose:
  Unattended scaling sweep on AWS: provision -> benchmark -> teardown.
  Produces end-to-end throughput and latency scaling plots for poster use.

Options:
  --seed <url>                    Seed URL (default: https://atlas.cs.brown.edu/data/gutenberg/)
  --nodeCounts <csv>              Node counts (default: 1,3,6,12)
  --repetitions <n>               Repetitions per node count (default: 3)
  --resultsDir <path>             Base output directory (default: results/perf)
  --sweepName <name>              Output folder prefix (default: m6_scaling)
  --maxDepth <n>                  Forwarded to m6_benchmark.js
  --maxPages <n>                  Forwarded to m6_benchmark.js
  --queryRuns <n>                 Forwarded to m6_benchmark.js
  --storageOps <n>                Forwarded to m6_benchmark.js
  --rankingRuns <n>               Forwarded to m6_benchmark.js
  --componentTimeoutMs <n>        Forwarded to m6_benchmark.js
  --runTimeoutSec <n>             Hard timeout per benchmark run (default: 600)
  --throughputMetric <mode>       Throughput metric: auto|endToEnd|bottleneck (default: auto)
  --spawnWorkers <bool>           Forwarded to m6_benchmark.js (default: false)
  --stopWorkers <bool>            Forwarded to m6_benchmark.js (default: false)

  --imageId <ami>                 Forwarded to deploy.sh
  --instanceType <type>           Forwarded to deploy.sh
  --keyName <name>                Forwarded to deploy.sh
  --securityGroup <sg-id>         Forwarded to deploy.sh
  --subnetId <subnet-id>          Forwarded to deploy.sh
  --region <region>               Forwarded to deploy.sh and teardown
  --workerPort <port>             Worker service port on each node (default: 8080)
  --userDataFile <path>           Forwarded to deploy.sh
  --sshUser <name>                Forwarded to deploy.sh (default: ubuntu)
  --sshKeyFile <path>             Forwarded to deploy.sh
  --waitSshTimeoutSec <n>         Forwarded to deploy.sh (default: 600)
  --noWaitSsh                     Forwarded to deploy.sh

  --provisionRetries <n>          Retry attempts for provisioning (default: 1)
  --benchmarkRetries <n>          Retry attempts for benchmark stage (default: 1)
  --keepInstancesOnFailure <bool> Keep instances if a run fails (default: false)
  --noWaitTerminate               Do not wait for instance-terminated after terminate call
  --help                          Show this message
`);
}

function parseNodeCounts(raw) {
  const parsed = String(raw || "")
    .split(",")
    .map((part) => Number(part.trim()))
    .filter((n) => Number.isFinite(n) && n >= 1);
  return Array.from(new Set(parsed));
}

function runCommand(command, args, options = {}) {
  const {
    timeoutMs,
    stdio,
    ...spawnOptions
  } = options;
  const out = spawnSync(command, args, {
    cwd: process.cwd(),
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    ...(Number.isFinite(timeoutMs) ? { timeout: timeoutMs } : {}),
    ...(stdio ? { stdio } : {}),
    ...spawnOptions,
  });

  if (out.error) {
    throw out.error;
  }

  if (out.status !== 0) {
    const stderr = String(out.stderr || "");
    const stdout = String(out.stdout || "");
    throw new Error(
      `${command} ${args.join(" ")} failed with status ${out.status}\n${stderr || stdout}`,
    );
  }

  return out;
}

function runWithRetries({ attempts, label, fn }) {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return fn(attempt);
    } catch (error) {
      lastError = error;
      console.error(
        `[sweep] ${label} attempt ${attempt}/${attempts} failed: ${error.message || error}`,
      );
      if (attempt < attempts) {
        console.error(`[sweep] retrying ${label}...`);
      }
    }
  }
  throw lastError;
}

function deployRun({
  runDir,
  nodeCount,
  deployOptions,
}) {
  const deployMetaPath = path.join(runDir, "deploy_metadata.json");
  const nodesPath = path.join(runDir, "nodes.json");

  const args = [
    path.join("scripts", "deploy.sh"),
    "--count",
    String(nodeCount),
    "--output",
    deployMetaPath,
    "--nodes-file",
    nodesPath,
    "--worker-port",
    String(deployOptions.workerPort),
  ];

  if (deployOptions.imageId) {
    args.push("--image-id", deployOptions.imageId);
  }
  if (deployOptions.instanceType) {
    args.push("--instance-type", deployOptions.instanceType);
  }
  if (deployOptions.keyName) {
    args.push("--key-name", deployOptions.keyName);
  }
  if (deployOptions.securityGroup) {
    args.push("--security-group", deployOptions.securityGroup);
  }
  if (deployOptions.subnetId) {
    args.push("--subnet-id", deployOptions.subnetId);
  }
  if (deployOptions.region) {
    args.push("--region", deployOptions.region);
  }
  if (deployOptions.userDataFile) {
    args.push("--user-data-file", deployOptions.userDataFile);
  }
  if (deployOptions.sshUser) {
    args.push("--ssh-user", deployOptions.sshUser);
  }
  if (deployOptions.sshKeyFile) {
    args.push("--ssh-key-file", deployOptions.sshKeyFile);
  }
  if (Number.isFinite(deployOptions.waitSshTimeoutSec)) {
    args.push("--wait-ssh-timeout-sec", String(deployOptions.waitSshTimeoutSec));
  }
  if (deployOptions.noWaitSsh) {
    args.push("--no-wait-ssh");
  }

  runCommand("bash", args, { stdio: "inherit" });

  const deployMeta = JSON.parse(fs.readFileSync(deployMetaPath, "utf8"));
  return {
    deployMeta,
    deployMetaPath,
    nodesPath,
    instanceIds: Array.isArray(deployMeta.instanceIds)
      ? deployMeta.instanceIds
      : [],
  };
}

function findLatestArtifactPath(rootDir, artifactName) {
  if (!fs.existsSync(rootDir)) {
    return null;
  }

  const stack = [rootDir];
  let latestPath = null;
  let latestMtime = 0;

  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    entries.forEach((entry) => {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        return;
      }
      if (entry.isFile() && entry.name === artifactName) {
        const stat = fs.statSync(fullPath);
        const mtime = Number(stat.mtimeMs || 0);
        if (mtime >= latestMtime) {
          latestMtime = mtime;
          latestPath = fullPath;
        }
      }
    });
  }

  return latestPath;
}

function terminateInstances(instanceIds, region, waitForTermination = true) {
  if (!instanceIds || instanceIds.length === 0) {
    return;
  }

  const regionArgs = region ? ["--region", region] : [];
  runCommand("aws", [
    ...regionArgs,
    "ec2",
    "terminate-instances",
    "--instance-ids",
    ...instanceIds,
  ]);

  if (!waitForTermination) {
    return;
  }

  runCommand("aws", [
    ...regionArgs,
    "ec2",
    "wait",
    "instance-terminated",
    "--instance-ids",
    ...instanceIds,
  ]);
}

function runBenchmark({
  runDir,
  seed,
  nodesPath,
  benchmarkOptions,
  runLabel,
  experimentId,
  nodeCount,
  repetition,
  runTimeoutSec,
}) {
  const benchDir = path.join(runDir, "benchmark");
  ensureDirectory(benchDir);

  const gid = `scale_${experimentId}_n${nodeCount}_r${repetition}`;
  const args = [
    path.join("scripts", "m6_benchmark.js"),
    "--seed",
    seed,
    "--gid",
    gid,
    "--indexGid",
    `index_${gid}`,
    "--nodesFile",
    nodesPath,
    "--spawnWorkers",
    String(Boolean(benchmarkOptions.spawnWorkers)),
    "--stopWorkers",
    String(Boolean(benchmarkOptions.stopWorkers)),
    "--resultsDir",
    benchDir,
    "--runLabel",
    runLabel,
    "--experimentId",
    experimentId,
  ];

  if (Number.isFinite(benchmarkOptions.maxDepth)) {
    args.push("--maxDepth", String(benchmarkOptions.maxDepth));
  }
  if (Number.isFinite(benchmarkOptions.maxPages)) {
    args.push("--maxPages", String(benchmarkOptions.maxPages));
  }
  if (Number.isFinite(benchmarkOptions.queryRuns)) {
    args.push("--queryRuns", String(benchmarkOptions.queryRuns));
  }
  if (Number.isFinite(benchmarkOptions.storageOps)) {
    args.push("--storageOps", String(benchmarkOptions.storageOps));
  }
  if (Number.isFinite(benchmarkOptions.rankingRuns)) {
    args.push("--rankingRuns", String(benchmarkOptions.rankingRuns));
  }
  if (Number.isFinite(benchmarkOptions.componentTimeoutMs)) {
    args.push("--componentTimeoutMs", String(benchmarkOptions.componentTimeoutMs));
  }

  const timeoutMs = Math.max(1, Number(runTimeoutSec || 600)) * 1000;
  const out = spawnSync("node", args, {
    cwd: process.cwd(),
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    timeout: timeoutMs,
  });

  const timedOut = Boolean(
    out.error && (out.error.code === "ETIMEDOUT" || out.error.signal === "SIGTERM"),
  );

  if (out.error && !timedOut) {
    throw out.error;
  }

  if (out.status !== 0 && !timedOut) {
    throw new Error(String(out.stderr || out.stdout || "benchmark command failed"));
  }

  const stdout = String(out.stdout || "");
  const first = stdout.indexOf("{");
  const last = stdout.lastIndexOf("}");

  let summaryPath = null;
  if (first !== -1 && last !== -1 && last >= first) {
    try {
      const parsed = JSON.parse(stdout.slice(first, last + 1));
      summaryPath = parsed?.artifacts?.summaryPath || null;
    } catch (_error) {
      summaryPath = null;
    }
  }

  if (!summaryPath || !fs.existsSync(summaryPath)) {
    const fallback = findLatestArtifactPath(benchDir, "checkpoint_summary.json");
    if (fallback && fs.existsSync(fallback)) {
      summaryPath = fallback;
    }
  }

  if (!summaryPath || !fs.existsSync(summaryPath)) {
    throw new Error("benchmark finished but no summary/checkpoint artifact path was found");
  }

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));

  return {
    summaryPath,
    summary,
    timedOut,
  };
}

function normalizeThroughputMetric(raw) {
  const normalized = String(raw || "auto").trim().toLowerCase();
  if (["endtoend", "e2e", "workflow"].includes(normalized)) {
    return "endToEnd";
  }
  if (["bottleneck", "component", "slowest-component"].includes(normalized)) {
    return "bottleneck";
  }
  return "auto";
}

function workflowMetricsFromSummary(summary, throughputMetricMode = "auto") {
  const endToEndMs = Number(summary?.details?.endToEndMs || 0);
  const endToEndThroughput = Number(summary?.details?.workflowThroughputPerSec || 0);
  const pagesFetched = Number(summary?.crawlIndexSummary?.pagesFetched || 0);
  const docs = Number(summary?.crawlIndexSummary?.docs || 0);
  const workflowItems = Number(summary?.details?.workflowItems || Math.max(pagesFetched, docs, 0));

  const computedEndToEndThroughput = endToEndThroughput > 0
    ? endToEndThroughput
    : endToEndMs > 0
      ? (workflowItems * 1000) / endToEndMs
      : 0;

  const componentThroughputs = Array.isArray(summary?.components)
    ? summary.components
      .map((component) => Number(component?.throughputPerSec || 0))
      .filter((n) => Number.isFinite(n) && n > 0)
    : [];
  const bottleneckThroughputPerSec = componentThroughputs.length > 0
    ? Math.min(...componentThroughputs)
    : 0;

  const mode = normalizeThroughputMetric(throughputMetricMode);
  let throughputPerSec = computedEndToEndThroughput;
  let throughputSource = "endToEnd";

  if (mode === "bottleneck") {
    throughputPerSec = bottleneckThroughputPerSec;
    throughputSource = "bottleneck";
  } else if (mode === "auto") {
    if (throughputPerSec <= 0 && bottleneckThroughputPerSec > 0) {
      throughputPerSec = bottleneckThroughputPerSec;
      throughputSource = "bottleneck";
    }
  }


  return {
    endToEndMs,
    throughputPerSec,
    throughputSource,
    throughputMode: mode,
    endToEndThroughputPerSec: computedEndToEndThroughput,
    bottleneckThroughputPerSec,
    workflowItems,
    pagesFetched,
    docs,
  };
}

function renderScalingLineChartSvg({
  title,
  yLabel,
  points,
  valueKey,
  color,
}) {
  const width = 1040;
  const height = 560;
  const padLeft = 95;
  const padRight = 40;
  const padTop = 70;
  const padBottom = 95;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;

  const sorted = [...points].sort((a, b) => a.nodeCount - b.nodeCount);
  const values = sorted.map((point) => Number(point[valueKey] || 0));
  const maxValue = Math.max(...values, 1);
  const xStep = sorted.length > 1 ? plotWidth / (sorted.length - 1) : 0;

  const pathD = sorted
    .map((point, idx) => {
      const x = padLeft + idx * xStep;
      const y = padTop + plotHeight - (Number(point[valueKey] || 0) / maxValue) * plotHeight;
      return `${idx === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");

  const circles = sorted
    .map((point, idx) => {
      const value = Number(point[valueKey] || 0);
      const x = padLeft + idx * xStep;
      const y = padTop + plotHeight - (value / maxValue) * plotHeight;
      return `<circle cx="${x}" cy="${y}" r="6" fill="${color}" />\n<text x="${x}" y="${y - 10}" text-anchor="middle" font-size="12" fill="#222">${round(value)}</text>\n<text x="${x}" y="${padTop + plotHeight + 24}" text-anchor="middle" font-size="14" fill="#222">${point.nodeCount}</text>`;
    })
    .join("\n");

  const yTicks = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const y = padTop + plotHeight - ratio * plotHeight;
      const value = round(maxValue * ratio);
      return `<line x1="${padLeft}" y1="${y}" x2="${padLeft + plotWidth}" y2="${y}" stroke="#ddd" />\n<text x="${padLeft - 10}" y="${y + 4}" text-anchor="end" font-size="12" fill="#333">${value}</text>`;
    })
    .join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="#fff" />
  <text x="${width / 2}" y="34" text-anchor="middle" font-size="26" font-family="Helvetica, Arial, sans-serif" fill="#111">${title}</text>
  ${yTicks}
  <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${padTop + plotHeight}" stroke="#444" />
  <line x1="${padLeft}" y1="${padTop + plotHeight}" x2="${padLeft + plotWidth}" y2="${padTop + plotHeight}" stroke="#444" />
  <path d="${pathD}" fill="none" stroke="${color}" stroke-width="3" />
  ${circles}
  <text x="${width / 2}" y="${height - 26}" text-anchor="middle" font-size="18" fill="#222">Node Count</text>
  <text x="30" y="${padTop + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 30 ${padTop + plotHeight / 2})" font-size="16" fill="#222">${yLabel}</text>
</svg>`;
}

function writeSweepArtifacts({
  outDir,
  experimentId,
  seed,
  nodeCounts,
  repetitions,
  runRecords,
  throughputMetric,
}) {
  const successful = runRecords.filter((run) => run.status === "success");

  const aggregates = nodeCounts
    .map((nodeCount) => {
      const runs = successful.filter((run) => run.nodeCount === nodeCount);
      const throughputValues = runs.map((run) => Number(run.metrics.throughputPerSec || 0));
      const latencyValues = runs.map((run) => Number(run.metrics.endToEndMs || 0));

      return {
        nodeCount,
        runs: runs.length,
        throughputMean: average(throughputValues),
        throughputStddev: stddev(throughputValues),
        throughputMin: throughputValues.length ? Math.min(...throughputValues) : 0,
        throughputMax: throughputValues.length ? Math.max(...throughputValues) : 0,
        latencyMeanMs: average(latencyValues),
        latencyStddevMs: stddev(latencyValues),
        latencyMinMs: latencyValues.length ? Math.min(...latencyValues) : 0,
        latencyMaxMs: latencyValues.length ? Math.max(...latencyValues) : 0,
      };
    })
    .filter((row) => row.runs > 0)
    .sort((a, b) => a.nodeCount - b.nodeCount);

  const throughputSvg = renderScalingLineChartSvg({
    title: `Workflow Throughput Scaling (${throughputMetric})`,
    yLabel: "Throughput (items/sec)",
    points: aggregates,
    valueKey: "throughputMean",
    color: "#2f74b5",
  });

  const latencySvg = renderScalingLineChartSvg({
    title: "End-to-End Workflow Latency Scaling",
    yLabel: "Latency (ms)",
    points: aggregates,
    valueKey: "latencyMeanMs",
    color: "#e4572e",
  });

  const throughputPlotPath = path.join(outDir, "workflow_throughput_scaling.svg");
  const latencyPlotPath = path.join(outDir, "workflow_latency_scaling.svg");
  const summaryPath = path.join(outDir, "scaling_summary.json");
  const csvPath = path.join(outDir, "scaling_metrics.csv");
  const htmlPath = path.join(outDir, "index.html");

  fs.writeFileSync(throughputPlotPath, throughputSvg, "utf8");
  fs.writeFileSync(latencyPlotPath, latencySvg, "utf8");

  const csvLines = [
    "node_count,runs,throughput_mean_items_per_sec,throughput_stddev,throughput_min,throughput_max,latency_mean_ms,latency_stddev_ms,latency_min_ms,latency_max_ms",
    ...aggregates.map(
      (row) =>
        `${row.nodeCount},${row.runs},${round(row.throughputMean)},${round(row.throughputStddev)},${round(row.throughputMin)},${round(row.throughputMax)},${round(row.latencyMeanMs)},${round(row.latencyStddevMs)},${round(row.latencyMinMs)},${round(row.latencyMaxMs)}`,
    ),
  ];
  fs.writeFileSync(csvPath, `${csvLines.join("\n")}\n`, "utf8");

  const summary = {
    generatedAt: new Date().toISOString(),
    experimentId,
    seed,
    throughputMetric,
    requestedNodeCounts: nodeCounts,
    requestedRepetitions: repetitions,
    completedRuns: successful.length,
    totalRuns: runRecords.length,
    aggregates,
    runs: runRecords,
    artifacts: {
      summaryPath,
      csvPath,
      throughputPlotPath,
      latencyPlotPath,
      htmlPath,
    },
  };
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2), "utf8");

  const tableRows = aggregates
    .map(
      (row) =>
        `<tr><td>${row.nodeCount}</td><td>${row.runs}</td><td>${round(row.throughputMean)}</td><td>${round(row.throughputStddev)}</td><td>${round(row.latencyMeanMs)}</td><td>${round(row.latencyStddevMs)}</td></tr>`,
    )
    .join("\n");

  const html = `<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>M6 Scaling Sweep</title>
<style>
body { font-family: Helvetica, Arial, sans-serif; margin: 20px; color: #111; }
h1 { margin-bottom: 4px; }
.meta { color: #555; margin-bottom: 10px; }
img { width: 100%; max-width: 1040px; border: 1px solid #ddd; margin-bottom: 16px; }
table { border-collapse: collapse; min-width: 820px; }
th, td { border: 1px solid #ddd; padding: 8px 10px; text-align: left; }
th { background: #f8f8f8; }
</style>
</head>
<body>
  <h1>M6 Workflow Scaling</h1>
  <div class="meta">Generated: ${summary.generatedAt}</div>
  <div class="meta">Seed: ${seed}</div>
  <div class="meta">Experiment: ${experimentId}</div>
  <div class="meta">Throughput Metric: ${throughputMetric}</div>
  <h2>Throughput vs Node Count</h2>
  <img src="workflow_throughput_scaling.svg" alt="Throughput scaling chart" />
  <h2>Latency vs Node Count</h2>
  <img src="workflow_latency_scaling.svg" alt="Latency scaling chart" />
  <h2>Aggregated Metrics</h2>
  <table>
    <thead>
      <tr>
        <th>Nodes</th>
        <th>Successful Runs</th>
        <th>Throughput Mean (items/s)</th>
        <th>Throughput Stddev</th>
        <th>Latency Mean (ms)</th>
        <th>Latency Stddev (ms)</th>
      </tr>
    </thead>
    <tbody>
      ${tableRows}
    </tbody>
  </table>
</body>
</html>`;
  fs.writeFileSync(htmlPath, html, "utf8");

  return summary;
}

function main() {
  if (hasFlag("help")) {
    printUsage();
    process.exitCode = 0;
    return;
  }

  const seed = readFlag("seed") || "https://atlas.cs.brown.edu/data/gutenberg/";
  const nodeCounts = parseNodeCounts(readFlag("nodeCounts") || "1,3,6,12");
  const repetitions = Math.max(1, readNumber("repetitions", 3));
  const resultsBaseDir =
    readFlag("resultsDir") || path.join(process.cwd(), "results", "perf");
  const sweepName = readFlag("sweepName") || "m6_scaling";
  const experimentId = `${sweepName}_${timestamp()}`;
  const outDir = path.join(resultsBaseDir, experimentId);
  ensureDirectory(outDir);

  if (nodeCounts.length === 0) {
    throw new Error("nodeCounts must include at least one positive integer");
  }

  const deployOptions = {
    imageId: readFlag("imageId"),
    instanceType: readFlag("instanceType"),
    keyName: readFlag("keyName"),
    securityGroup: readFlag("securityGroup"),
    subnetId: readFlag("subnetId"),
    region: readFlag("region"),
    workerPort: Math.max(1, readNumber("workerPort", 8080)),
    userDataFile: readFlag("userDataFile"),
    sshUser: readFlag("sshUser") || "ubuntu",
    sshKeyFile: readFlag("sshKeyFile"),
    waitSshTimeoutSec: Math.max(30, readNumber("waitSshTimeoutSec", 600)),
    noWaitSsh: hasFlag("noWaitSsh"),
  };

  const benchmarkOptions = {
    maxDepth: readNumber("maxDepth", 1),
    maxPages: readNumber("maxPages", 30),
    queryRuns: readNumber("queryRuns", 20),
    storageOps: readNumber("storageOps", 50),
    rankingRuns: readNumber("rankingRuns", 80),
    componentTimeoutMs: readNumber("componentTimeoutMs", 120000),
    runTimeoutSec: Math.max(30, readNumber("runTimeoutSec", 600)),
    throughputMetric: normalizeThroughputMetric(readFlag("throughputMetric") || "auto"),
    spawnWorkers: hasExplicitFlag("spawnWorkers")
      ? readBoolean("spawnWorkers", false)
      : false,
    stopWorkers: hasExplicitFlag("stopWorkers")
      ? readBoolean("stopWorkers", false)
      : false,
  };

  const provisionRetries = Math.max(1, readNumber("provisionRetries", 1));
  const benchmarkRetries = Math.max(1, readNumber("benchmarkRetries", 1));
  const keepInstancesOnFailure = readBoolean("keepInstancesOnFailure", false);
  const waitTerminate = !hasFlag("noWaitTerminate");

  console.error(
    `[sweep] config seed=${seed} nodeCounts=${nodeCounts.join(",")} repetitions=${repetitions} runTimeoutSec=${benchmarkOptions.runTimeoutSec} waitSshTimeoutSec=${deployOptions.waitSshTimeoutSec} noWaitSsh=${deployOptions.noWaitSsh} waitTerminate=${waitTerminate} throughputMetric=${benchmarkOptions.throughputMetric} spawnWorkers=${benchmarkOptions.spawnWorkers} stopWorkers=${benchmarkOptions.stopWorkers}`,
  );

  const runRecords = [];

  for (const nodeCount of nodeCounts) {
    for (let repetition = 1; repetition <= repetitions; repetition++) {
      const runTag = `n${nodeCount}_r${repetition}`;
      const runDir = path.join(outDir, runTag);
      ensureDirectory(runDir);

      console.error(`[sweep] starting ${runTag}`);
      const record = {
        nodeCount,
        repetition,
        runTag,
        status: "failed",
      };

      let instanceIds = [];
      let deployMetaPath = null;
      let summaryPath = null;

      try {
        console.error(`[sweep] ${runTag} provisioning ${nodeCount} node(s)...`);
        const deployment = runWithRetries({
          attempts: provisionRetries,
          label: `${runTag} provisioning`,
          fn: () =>
            deployRun({
              runDir,
              nodeCount,
              deployOptions,
            }),
        });

        instanceIds = deployment.instanceIds;
        deployMetaPath = deployment.deployMetaPath;

        const benchmark = runWithRetries({
          attempts: benchmarkRetries,
          label: `${runTag} benchmark`,
          fn: () =>
            runBenchmark({
              runDir,
              seed,
              nodesPath: deployment.nodesPath,
              benchmarkOptions,
              runLabel: runTag,
              experimentId,
              nodeCount,
              repetition,
              runTimeoutSec: benchmarkOptions.runTimeoutSec,
            }),
        });

        summaryPath = benchmark.summaryPath;
        const metrics = workflowMetricsFromSummary(
          benchmark.summary,
          benchmarkOptions.throughputMetric,
        );
        record.status = "success";
        record.metrics = metrics;
        record.benchmarkTimedOut = Boolean(benchmark.timedOut);
        record.summaryPath = summaryPath;
        record.deployMetaPath = deployMetaPath;
        record.instanceIds = instanceIds;
        console.error(
          `[sweep] completed ${runTag}: throughput=${round(metrics.throughputPerSec)} items/s source=${metrics.throughputSource} latency=${round(metrics.endToEndMs)} ms timedOut=${record.benchmarkTimedOut}`,
        );
      } catch (error) {
        record.error = String(error && (error.stack || error.message || error));
        record.deployMetaPath = deployMetaPath;
        record.summaryPath = summaryPath;
        record.instanceIds = instanceIds;
        console.error(`[sweep] failed ${runTag}: ${record.error}`);
      } finally {
        if (!keepInstancesOnFailure || record.status === "success") {
          try {
            terminateInstances(instanceIds, deployOptions.region, waitTerminate);
            if (instanceIds.length > 0) {
              console.error(`[sweep] terminated instances for ${runTag}`);
            }
          } catch (termError) {
            record.teardownError = String(
              termError && (termError.stack || termError.message || termError),
            );
            console.error(`[sweep] teardown issue for ${runTag}: ${record.teardownError}`);
          }
        }
      }

      runRecords.push(record);
      fs.writeFileSync(
        path.join(outDir, "runs_progress.json"),
        JSON.stringify(runRecords, null, 2),
        "utf8",
      );
    }
  }

  const summary = writeSweepArtifacts({
    outDir,
    experimentId,
    seed,
    nodeCounts,
    repetitions,
    runRecords,
    throughputMetric: benchmarkOptions.throughputMetric,
  });

  console.log(JSON.stringify(summary, null, 2));
}

main();
