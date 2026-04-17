#!/usr/bin/env node

const path = require('path');
require(path.join(__dirname, '..', 'distribution.js'))();
const distribution = globalThis.distribution;

const {queryIndex} = require('./pipeline.js');
const {
  hasRealError,
  getNodes,
  stopNodes,
  spawnNodes,
  setupGroups,
  shutdown,
} = require('./cluster.js');

const NODES = getNodes();

const query = process.argv.slice(2).join(' ');
if (!query) {
  console.error('Usage: node m6/query.js <query terms>');
  process.exit(1);
}

distribution.node.start((e) => {
  if (hasRealError(e)) {
    console.error('node.start failed:', e);
    shutdown(distribution, NODES, 1);
    return;
  }

  stopNodes(distribution, NODES, () => {
    spawnNodes(distribution, NODES, () => {
      setupGroups(distribution, NODES, ['ridx', 'rlg'], (groupErr) => {
        if (hasRealError(groupErr)) {
          console.error('query group setup failed:', groupErr);
          shutdown(distribution, NODES, 1);
          return;
        }

        queryIndex(distribution, {rlgGid: 'rlg', ridxGid: 'ridx', topK: 10}, query, (queryErr, ranked) => {
          if (hasRealError(queryErr)) {
            console.error('query failed:', queryErr);
            shutdown(distribution, NODES, 1);
            return;
          }

          if (!ranked || ranked.length === 0) {
            console.log('No results.');
          } else {
            ranked.forEach((row, i) => {
              const label = row.url || row.docId;
              console.log(`${i + 1}. ${label} score=${row.score}`);
            });
          }

          shutdown(distribution, NODES, 0);
        });
      });
    });
  });
});
