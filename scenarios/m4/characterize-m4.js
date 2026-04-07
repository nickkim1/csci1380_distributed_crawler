#!/usr/bin/env node

/**
 * cloud storage characterization
 */

const path = require('path');
require(path.join(__dirname, '..', '..', 'distribution.js'))();
const distribution = globalThis.distribution;
const id = distribution.util.id;
const crypto = require('crypto');


// configuration
const GID = 'benchgroup';
const COUNT = 1000;

// const NODES = [
//   {ip: '127.0.0.1', port: 8000},
//   {ip: '127.0.0.1', port: 8001},
//   {ip: '127.0.0.1', port: 8002},
// ];
const NODES = [
  {ip: '52.205.85.193', port: 8080},
  {ip: '100.54.121.87', port: 8082},
  {ip: '44.204.208.187', port: 8081},
];

// helpers
function randomString(len) {
  return crypto.randomBytes(len).toString('hex');
}

function randomObject() {
  return {
    id: randomString(8),
    value: Math.random() * 1000,
    tags: [randomString(4), randomString(4)],
    nested: {a: Math.random(), b: randomString(4)},
  };
}

function printStats(title, latencies, totalTimeMs) {
  const n = latencies.length;
  const avg = latencies.reduce((a, b) => a + b, 0) / n;
  const throughput = n / totalTimeMs;
  console.log(`===== ${title} =====`);
  console.log(`throughput : ${throughput.toFixed(4)} ops/ms`);
  console.log(`latency : ${avg.toFixed(4)} ms`);
}

// local setup/teardown
function stopNodes(callback) {
  let i = 0;
  function next() {
    if (i >= NODES.length) return callback();
    distribution.local.comm.send(
        [], {node: NODES[i], service: 'status', method: 'stop'},
        () => {
          i++; next();
        });
  }
  next();
}

function spawnNodes(callback) {
  let i = 0;
  function next() {
    if (i >= NODES.length) return callback();
    distribution.local.status.spawn(NODES[i], (e) => {
      if (e) console.error(`Warning: spawn failed for ${NODES[i].ip}:${NODES[i].port}:`, e);
      i++; next();
    });
  }
  next();
}

function setupGroup(gid, callback) {
  const group = {};
  NODES.forEach((n) => {
    group[id.getSID(n)] = n;
  });
  const config = {gid};
  distribution.local.groups.put(config, group, () => {
    distribution[gid].groups.put(config, group, callback);
  });
}

// benchmark stages
function generateData() {
  console.log(`generating ${COUNT} key-value pairs`);
  const data = [];
  for (let i = 0; i < COUNT; i++) {
    data.push({key: `bench_${i}_${randomString(6)}`, value: randomObject()});
  }
  console.log(`done!`);
  return data;
}

function benchmarkPut(gid, data, callback) {
  console.log(`inserting ${COUNT} objects`);
  const latencies = [];
  let i = 0;
  const start = performance.now();
  function next() {
    if (i >= data.length) {
      const total = performance.now() - start;
      printStats('PUT', latencies, total);
      return callback();
    }
    const {key, value} = data[i];
    const t0 = performance.now();
    distribution[gid].store.put(value, key, (e) => {
      latencies.push(performance.now() - t0);
      if (e) console.error(`put error [${key}]:`, e);
      i++;
      next();
    });
  }
  next();
}

function benchmarkGet(gid, data, callback) {
  console.log(`querying ${COUNT} objects`);
  const latencies = [];
  let i = 0;
  const start = performance.now();
  function next() {
    if (i >= data.length) {
      const total = performance.now() - start;
      printStats('GET', latencies, total);
      if (errors > 0) console.log(`  Errors: ${errors}`);
      return callback();
    }
    const {key} = data[i];
    const t0 = performance.now();
    distribution[gid].store.get(key, (e) => {
      latencies.push(performance.now() - t0);
      if (e) console.error(`get error [${key}]:`, e);
      i++;
      next();
    });
  }
  next();
}

// engine(s)

// local
// distribution.node.start(() => {
//   stopNodes(() => {
//     spawnNodes(() => {
//       setupGroup(GID, () => {
//         const data = generateData();
//         benchmarkPut(GID, data, () => {
//           benchmarkGet(GID, data, () => {
//             console.log('\nDone.');
//             stopNodes(() => {
//               if (globalThis.distribution.node.server) {
//                 globalThis.distribution.node.server.close();
//               }
//               process.exit(0);
//             });
//           });
//         });
//       });
//     });
//   });
// });

// aws
distribution.node.start(() => {
  setupGroup(GID, () => {
    const data = generateData();
    benchmarkPut(GID, data, () => {
      benchmarkGet(GID, data, () => {
        console.log('\nDone.');
        if (globalThis.distribution.node.server) {
          globalThis.distribution.node.server.close();
        }
        process.exit(0);
      });
    });
  });
});
