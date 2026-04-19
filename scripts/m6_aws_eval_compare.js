#!/usr/bin/env node
// @ts-nocheck

const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

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

function printUsage() {
  console.log(`Usage:
  node scripts/m6_aws_eval_compare.js --seed1 <url> --seed2 <url> [options]

Purpose:
  Run component benchmarks on two corpora and generate grouped latency/throughput bar charts
  with components: crawler, indexer, storage, search.
  (No page-rank or NLP columns.)

Required:
  --seed1 <url>          Corpus 1 seed URL
  --seed2 <url>          Corpus 2 seed URL

Optional:
  --label1 <text>        Legend label for corpus 1 (default: Corpus 1)
  --label2 <text>        Legend label for corpus 2 (default: Corpus 2)
  --resultsDir <path>    Base output dir (default: results/perf)
  --groupName <name>     Prefix for output folder (default: aws_eval_compare)
  --maxDepth <n>         Forwarded to m6_benchmark.js (default: 2)
  --maxPages <n>         Forwarded to m6_benchmark.js (default: 1000)
  --workerCount <n>      Forwarded to m6_benchmark.js
  --basePort <n>         Base port for run 1 local workers (default: 9361)
  --queryRuns <n>        Forwarded to m6_benchmark.js (default: 20)
  --storageOps <n>       Forwarded to m6_benchmark.js (default: 50)
  --nodes <csv>          Forwarded to m6_benchmark.js for external AWS nodes
  --nodesFile <path>     Forwarded to m6_benchmark.js for external AWS nodes
  --spawnWorkers <bool>  Forwarded (default determined by m6_benchmark.js)
  --stopWorkers <bool>   Forwarded (default determined by m6_benchmark.js)
  --refreshCache <bool>  Forwarded to both runs
  --useCache <bool>      Forwarded to both runs
  --queries <csv>        Forwarded to both runs
  --help                 Show this message

Example AWS run:
  node scripts/m6_aws_eval_compare.js \
    --seed1 https://cs.brown.edu/courses/csci1380/sandbox/3/ \
    --seed2 https://atlas.cs.brown.edu/data/gutenberg/ \
    --label1 Sandbox3 --label2 Gutenberg \
    --nodesFile ./aws_nodes.json --spawnWorkers false --stopWorkers false \
    --maxPages 120000 --maxDepth 8 --queryRuns 50 --storageOps 200
`);
}

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

function round(n) {
  return Number((Number(n) || 0).toFixed(4));
}

function renderGroupedBarChartSvg({ title, yLabel, components, series, valueKey }) {
  const width = 1100;
  const height = 580;
  const padLeft = 95;
  const padRight = 30;
  const padTop = 80;
  const padBottom = 120;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;
  const palette = ["#f58518", "#2f74b5"];

  const allValues = [];
  series.forEach((s) => {
    components.forEach((c) => {
      allValues.push(Number(s.metrics[c]?.[valueKey] || 0));
    });
  });
  const max = Math.max(...allValues, 1);

  const groupGap = 24;
  const groupWidth = (plotWidth - groupGap * (components.length + 1)) / Math.max(components.length, 1);
  const barGap = 8;
  const barsPerGroup = Math.max(series.length, 1);
  const barWidth = Math.max(16, (groupWidth - barGap * (barsPerGroup + 1)) / barsPerGroup);

  const ticks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
    const y = padTop + plotHeight - ratio * plotHeight;
    const value = round(max * ratio);
    return `<line x1="${padLeft}" y1="${y}" x2="${padLeft + plotWidth}" y2="${y}" stroke="#ddd" />\n<text x="${padLeft - 10}" y="${y + 4}" text-anchor="end" font-size="12" fill="#333">${value}</text>`;
  }).join("\n");

  const groups = components.map((component, groupIdx) => {
    const gx = padLeft + groupGap + groupIdx * (groupWidth + groupGap);
    const cx = gx + groupWidth / 2;

    const bars = series.map((s, i) => {
      const raw = Number(s.metrics[component]?.[valueKey] || 0);
      const h = (raw / max) * plotHeight;
      const x = gx + barGap + i * (barWidth + barGap);
      const y = padTop + (plotHeight - h);
      return `<rect x="${x}" y="${y}" width="${barWidth}" height="${h}" fill="${palette[i % palette.length]}" />`;
    }).join("\n");

    return `${bars}\n<text x="${cx}" y="${padTop + plotHeight + 28}" text-anchor="middle" font-size="20" fill="#222">${component}</text>`;
  }).join("\n");

  const legend = series.map((s, i) => {
    const x = width - 320 + i * 140;
    const y = 26;
    return `<rect x="${x}" y="${y - 12}" width="20" height="20" fill="${palette[i % palette.length]}" />\n<text x="${x + 28}" y="${y + 3}" font-size="18" fill="#111">${s.label}</text>`;
  }).join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="#fff" />
  <text x="${width / 2}" y="40" text-anchor="middle" font-size="30" font-family="Helvetica, Arial, sans-serif" fill="#111">${title}</text>
  ${legend}
  ${ticks}
  <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${padTop + plotHeight}" stroke="#444" />
  <line x1="${padLeft}" y1="${padTop + plotHeight}" x2="${padLeft + plotWidth}" y2="${padTop + plotHeight}" stroke="#444" />
  ${groups}
  <text x="${width / 2}" y="${height - 24}" text-anchor="middle" font-size="24" fill="#222">System Components</text>
  <text x="28" y="${padTop + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 28 ${padTop + plotHeight / 2})" font-size="24" fill="#222">${yLabel}</text>
</svg>`;
}

function runSingleBenchmark({ seed, label, runId, gid, indexGid, resultDir, forwardedArgs }) {
  const cmdArgs = [
    path.join("scripts", "m6_benchmark.js"),
    "--seed", seed,
    "--gid", gid,
    "--indexGid", indexGid,
    "--resultsDir", resultDir,
    "--cacheFile", path.join(process.cwd(), ".cache", `m6_benchmark_${runId}_${gid}.json`),
    ...forwardedArgs,
  ];

  const stdout = execFileSync("node", cmdArgs, {
    cwd: process.cwd(),
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });

  const first = stdout.indexOf("{");
  const last = stdout.lastIndexOf("}");
  if (first === -1 || last === -1 || last < first) {
    throw new Error(`benchmark output did not contain JSON: ${stdout}`);
  }

  const parsed = JSON.parse(stdout.slice(first, last + 1));
  const summaryPath = parsed?.artifacts?.summaryPath;
  if (!summaryPath || !fs.existsSync(summaryPath)) {
    throw new Error(`summary.json missing for ${label}`);
  }

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  const byName = Object.fromEntries(
    (summary.components || []).map((c) => [c.name, c]),
  );

  return {
    label,
    seed,
    gid,
    indexGid,
    sourceSummaryPath: summaryPath,
    metrics: {
      crawler: byName.crawler || {},
      indexer: byName.indexer || {},
      storage: byName.storage || {},
      search: byName.search || {},
    },
    fullSummary: summary,
  };
}

function main() {
  if (hasFlag("help")) {
    printUsage();
    process.exitCode = 0;
    return;
  }

  const seed1 = readFlag("seed1");
  const seed2 = readFlag("seed2");
  if (!seed1 || !seed2) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  const label1 = readFlag("label1") || "Corpus 1";
  const label2 = readFlag("label2") || "Corpus 2";
  const resultsBase = readFlag("resultsDir") || path.join(process.cwd(), "results", "perf");
  const groupName = readFlag("groupName") || "aws_eval_compare";
  const runId = `${groupName}_${timestamp()}`;
  const outDir = path.join(resultsBase, runId);
  ensureDirectory(outDir);

  const forwardFlagNames = [
    "maxDepth",
    "maxPages",
    "workerCount",
    "basePort",
    "queryRuns",
    "storageOps",
    "queries",
    "nodes",
    "nodesFile",
    "spawnWorkers",
    "stopWorkers",
    "refreshCache",
    "useCache",
  ];

  const forwardedArgs = [];
  forwardFlagNames.forEach((name) => {
    const value = readFlag(name);
    if (value !== null) {
      forwardedArgs.push(`--${name}`, value);
    }
  });

  const basePort = readNumber("basePort", 9361);
  const run1 = runSingleBenchmark({
    seed: seed1,
    label: label1,
    runId,
    gid: `${groupName}_c1`,
    indexGid: `index_${groupName}_c1`,
    resultDir: path.join(outDir, "run_corpus1"),
    forwardedArgs,
  });

  // Shift basePort on the second run when local workers are used.
  const secondArgs = [...forwardedArgs];
  if (!readFlag("nodes") && !readFlag("nodesFile")) {
    const idx = secondArgs.indexOf("--basePort");
    if (idx >= 0) {
      secondArgs[idx + 1] = String(Number(secondArgs[idx + 1]) + 100);
    } else {
      secondArgs.push("--basePort", String(basePort + 100));
    }
  }

  const run2 = runSingleBenchmark({
    seed: seed2,
    label: label2,
    runId,
    gid: `${groupName}_c2`,
    indexGid: `index_${groupName}_c2`,
    resultDir: path.join(outDir, "run_corpus2"),
    forwardedArgs: secondArgs,
  });

  const components = ["crawler", "indexer", "storage", "search"];
  const latencySvg = renderGroupedBarChartSvg({
    title: "Component Latency Comparison",
    yLabel: "Latency (ms)",
    components,
    series: [run1, run2],
    valueKey: "latencyMs",
  });
  const throughputSvg = renderGroupedBarChartSvg({
    title: "Component Throughput Comparison",
    yLabel: "Throughput (/sec)",
    components,
    series: [run1, run2],
    valueKey: "throughputPerSec",
  });

  const latencyPath = path.join(outDir, "latency_compare.svg");
  const throughputPath = path.join(outDir, "throughput_compare.svg");
  const summaryPath = path.join(outDir, "compare_summary.json");
  const csvPath = path.join(outDir, "compare_metrics.csv");
  const htmlPath = path.join(outDir, "index.html");

  fs.writeFileSync(latencyPath, latencySvg, "utf8");
  fs.writeFileSync(throughputPath, throughputSvg, "utf8");

  const csvLines = [
    "corpus,component,latency_ms,throughput_per_sec,samples,unit",
  ];
  [run1, run2].forEach((run) => {
    components.forEach((name) => {
      const m = run.metrics[name] || {};
      csvLines.push(
        `${run.label},${name},${round(m.latencyMs)},${round(m.throughputPerSec)},${Number(m.samples || 0)},${m.unit || ""}`,
      );
    });
  });
  fs.writeFileSync(csvPath, `${csvLines.join("\n")}\n`, "utf8");

  const out = {
    generatedAt: new Date().toISOString(),
    outDir,
    comparedComponents: components,
    corpus1: {
      label: run1.label,
      seed: run1.seed,
      metrics: Object.fromEntries(
        components.map((c) => [
          c,
          {
            latencyMs: round(run1.metrics[c]?.latencyMs),
            throughputPerSec: round(run1.metrics[c]?.throughputPerSec),
            samples: Number(run1.metrics[c]?.samples || 0),
            unit: run1.metrics[c]?.unit || "",
          },
        ]),
      ),
      sourceSummaryPath: run1.sourceSummaryPath,
    },
    corpus2: {
      label: run2.label,
      seed: run2.seed,
      metrics: Object.fromEntries(
        components.map((c) => [
          c,
          {
            latencyMs: round(run2.metrics[c]?.latencyMs),
            throughputPerSec: round(run2.metrics[c]?.throughputPerSec),
            samples: Number(run2.metrics[c]?.samples || 0),
            unit: run2.metrics[c]?.unit || "",
          },
        ]),
      ),
      sourceSummaryPath: run2.sourceSummaryPath,
    },
    artifacts: {
      summaryPath,
      csvPath,
      latencyPath,
      throughputPath,
      htmlPath,
    },
  };

  fs.writeFileSync(summaryPath, JSON.stringify(out, null, 2), "utf8");

  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>AWS Eval Compare</title>
  <style>
    body { font-family: Helvetica, Arial, sans-serif; margin: 20px; color: #111; }
    h1 { margin-bottom: 6px; }
    .meta { color: #555; margin-bottom: 14px; }
    img { width: 100%; max-width: 1100px; border: 1px solid #ddd; margin-bottom: 16px; }
    table { border-collapse: collapse; min-width: 780px; }
    th, td { border: 1px solid #ddd; padding: 8px 10px; text-align: left; }
    th { background: #f8f8f8; }
  </style>
</head>
<body>
  <h1>AWS Two-Corpus Component Evaluation</h1>
  <div class="meta">Generated: ${out.generatedAt}</div>
  <div class="meta">Components: crawler, indexer, storage, search (page-rank and NLP excluded)</div>
  <h2>Latency Comparison</h2>
  <img src="latency_compare.svg" alt="Latency comparison chart" />
  <h2>Throughput Comparison</h2>
  <img src="throughput_compare.svg" alt="Throughput comparison chart" />
</body>
</html>`;
  fs.writeFileSync(htmlPath, html, "utf8");

  console.log(JSON.stringify(out, null, 2));
}

main();
