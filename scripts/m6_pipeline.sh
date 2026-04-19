#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "$#" -eq 0 ]]; then
  cat << 'EOF'
Run distributed crawl/index/query pipeline from CLI.

By default, this script clears store/ before each run to avoid cross-run contamination.
Set KEEP_STORE=1 to skip cleanup.

Usage:
  ./scripts/m6_pipeline.sh --seed <url> --query <text> [options]

Examples:
  ./scripts/m6_pipeline.sh --seed https://cs.brown.edu/courses/csci1380/sandbox/3/ --query "book summary"
  ./scripts/m6_pipeline.sh --seed https://cs.brown.edu/courses/csci1380/sandbox/3/ --query "mystery" --maxPages 30 --maxDepth 1

Options are forwarded to scripts/m6_pipeline_cli.js:
  --seed --query --gid --indexGid --maxDepth --maxPages --limit --workerCount --basePort
  --cacheFile --useCache --refreshCache
EOF
  exit 0
fi

cd "$repo_root"

if [[ "${KEEP_STORE:-0}" != "1" ]]; then
  mkdir -p "$repo_root/store"
  find "$repo_root/store" -mindepth 1 -delete
fi

node scripts/m6_pipeline_cli.js "$@"
