#!/usr/bin/env node

const path = require('path');
require(path.join(__dirname, '..', 'distribution.js'))();
const distribution = globalThis.distribution;

const {
  runPipeline,
  queryIndex,
} = require('./pipeline.js');

const {
  hasRealError,
  getNodes,
  stopNodes,
  spawnNodes,
  setupGroups,
  shutdown,
} = require('./cluster.js');

const NODES = getNodes();

const seeds = process.argv.slice(2);
const seedRecords = seeds.length > 0 ? seeds.map((url) => ({key: url, value: {url}})) : [
  {key: 'https://cs.brown.edu/courses/csci1380/sandbox/2/', value: {url: 'https://cs.brown.edu/courses/csci1380/sandbox/2/'}},
];

const maxDepth = Number(process.env.M6_CRAWL_DEPTH || 1);
const maxPages = Number(process.env.M6_MAX_PAGES || 20);
const allowCrossHost = process.env.M6_ALLOW_CROSS_HOST === '1';

distribution.node.start((startErr) => {
  if (hasRealError(startErr)) {
    console.error('node.start failed:', startErr);
    shutdown(distribution, NODES, 1);
    return;
  }

  stopNodes(distribution, NODES, () => {
    spawnNodes(distribution, NODES, () => {
      setupGroups(distribution, NODES, ['crawl', 'ridx', 'rlg'], (groupErr) => {
        if (hasRealError(groupErr)) {
          console.error('group setup failed:', groupErr);
          shutdown(distribution, NODES, 1);
          return;
        }

        runPipeline(distribution, {
          seedRecords,
          expandCrawl: true,
          maxDepth,
          maxPages,
          allowCrossHost,
        }, (pipelineErr, stats) => {
          if (hasRealError(pipelineErr)) {
            console.error('pipeline failed:', pipelineErr);
            shutdown(distribution, NODES, 1);
            return;
          }

          console.log(`crawl config: depth=${maxDepth} maxPages=${maxPages} allowCrossHost=${allowCrossHost}`);
          console.log('pipeline stats:', stats);
          queryIndex(distribution, {rlgGid: 'rlg', topK: 5}, 'distributed gutenberg', (queryErr, ranked) => {
            if (hasRealError(queryErr)) {
              console.error('query failed:', queryErr);
              shutdown(distribution, NODES, 1);
              return;
            }

            console.log('query results:', ranked);
            shutdown(distribution, NODES, 0);
          });
        });
      });
    });
  });
});
