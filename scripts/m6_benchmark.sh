#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "$#" -eq 0 ]]; then
  cat << 'EOF'
Run component-level performance benchmarking for crawler/indexer/storage/ranking/search.

Usage:
  ./scripts/m6_benchmark.sh --seed <url> [options]

Examples:
  ./scripts/m6_benchmark.sh --seed https://cs.brown.edu/courses/csci1380/sandbox/3/
  ./scripts/m6_benchmark.sh --seed https://cs.brown.edu/courses/csci1380/sandbox/3/ --maxPages 20 --queryRuns 15 --resultsDir results/perf

By default, this script clears store/ before each run to avoid cross-run contamination.
Set KEEP_STORE=1 to skip cleanup.

Forwarded options:
  --seed --gid --indexGid --queries --maxDepth --maxPages --workerCount --basePort
  --nodes --nodesFile --spawnWorkers --stopWorkers
  --queryRuns --storageOps --rankingRuns --resultsDir --cacheFile --useCache --refreshCache
EOF
  exit 0
fi

cd "$repo_root"

if [[ "${KEEP_STORE:-0}" != "1" ]]; then
  mkdir -p "$repo_root/store"
  find "$repo_root/store" -mindepth 1 -delete
fi

node scripts/m6_benchmark.js "$@"
