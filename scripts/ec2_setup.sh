#!/usr/bin/env bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

REPO_URL="${REPO_URL:-https://github.com/nickkim1/csci1380_distributed_crawler.git}"
REPO_DIR="${REPO_DIR:-/home/ubuntu/distributed-search-engine}"
WORKER_PORT="${WORKER_PORT:-8080}"
WORKER_IP="${WORKER_IP:-0.0.0.0}"

apt-get update -y
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs git vim shellcheck

if [[ ! -d "$REPO_DIR/.git" ]]; then
	sudo -u ubuntu git clone "$REPO_URL" "$REPO_DIR"
fi

cd "$REPO_DIR"
sudo -u ubuntu npm install --no-audit --no-fund
sudo -u ubuntu mkdir -p "$REPO_DIR/store" "$REPO_DIR/.cache"

cat >/etc/systemd/system/csci1380-worker.service <<EOF
[Unit]
Description=CSCI1380 Distribution Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=$REPO_DIR
ExecStart=/usr/bin/node $REPO_DIR/distribution.js --ip $WORKER_IP --port $WORKER_PORT
Restart=always
RestartSec=2
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now csci1380-worker.service
