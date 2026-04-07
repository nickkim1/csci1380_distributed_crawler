#!/bin/bash
cd /home/ubuntu
apt update
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs git vim shellcheck
GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"
git clone git@github.com:peter-popescu/distributed-search-engine.git
cd distributed-search-engine
npm i '@brown-ds/distribution'
