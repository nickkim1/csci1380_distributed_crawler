#!/usr/bin/env node

/**
 * local MapReduce characterization of distributed word count
 */

const path = require('path');
require(path.join(__dirname, '..', '..', 'distribution.js'))();
const distribution = globalThis.distribution;
const id = distribution.util.id;
const crypto = require('crypto');

const GID = 'm5bench';
const DOC_COUNT = 300;
const WORDS_PER_DOC = 80;
const MR_RUNS = 5;

const NODES = [
  {ip: '127.0.0.1', port: 7110},
  {ip: '127.0.0.1', port: 7111},
  {ip: '127.0.0.1', port: 7112},
];

function hasRealError(e) {
  if (!e) {
    return false;
  }
  if (e instanceof Error) {
    return true;
  }
  if (typeof e === 'object') {
    return Object.keys(e).length > 0;
  }
  return true;
}

function randomInt(max) {
  return Math.floor(Math.random() * max);
}

function randomId(len) {
  return crypto.randomBytes(len).toString('hex');
}

function buildVocabulary(size) {
  const words = [];
  for (let i = 0; i < size; i++) {
    words.push(`w${i}`);
  }
  return words;
}

function buildDoc(vocab, wordsPerDoc) {
  const out = [];
  for (let i = 0; i < wordsPerDoc; i++) {
    out.push(vocab[randomInt(vocab.length)]);
  }
  return out.join(' ');
}

function generateDataset(count, wordsPerDoc) {
  const vocab = buildVocabulary(200);
  const dataset = [];
  for (let i = 0; i < count; i++) {
    dataset.push({
      key: `doc_${i}_${randomId(4)}`,
      value: buildDoc(vocab, wordsPerDoc),
    });
  }
  return dataset;
}

function printMrStats(latencies, totalMs, docsPerRun) {
  const n = latencies.length;
  const avg = n ? latencies.reduce((a, b) => a + b, 0) / n : 0;
  const jobsPerMs = totalMs > 0 ? n / totalMs : 0;

  console.log('===== MR EXEC (WORD COUNT) =====');
  console.log(`runs           : ${n}`);
  console.log(`throughput     : ${jobsPerMs.toFixed(6)} jobs/ms`);
  console.log(`latency avg    : ${avg.toFixed(4)} ms`);
}

function stopNodes(callback) {
  let i = 0;
  function next() {
    if (i >= NODES.length) {
      callback();
      return;
    }
    const remote = {node: NODES[i], service: 'status', method: 'stop'};
    distribution.local.comm.send([], remote, () => {
      i += 1;
      next();
    });
  }
  next();
}

function spawnNodes(callback) {
  let i = 0;
  function next() {
    if (i >= NODES.length) {
      callback();
      return;
    }
    distribution.local.status.spawn(NODES[i], (e) => {
      if (e) {
        console.error(`spawn warning for ${NODES[i].ip}:${NODES[i].port}:`, e);
      }
      i += 1;
      next();
    });
  }
  next();
}

function setupGroup(gid, callback) {
  const group = {};
  NODES.forEach((node) => {
    group[id.getSID(node)] = node;
  });
  const config = {gid};
  distribution.local.groups.put(config, group, (e) => {
    if (hasRealError(e)) {
      callback(e);
      return;
    }
    distribution[gid].groups.put(config, group, (e2, v2) => {
      if (hasRealError(e2)) {
        callback(e2);
        return;
      }
      callback(null, v2);
    });
  });
}

function loadDataset(gid, dataset, callback) {
  console.log(`loading ${dataset.length} docs into ${gid}`);
  let i = 0;

  function next() {
    if (i >= dataset.length) {
      callback(null);
      return;
    }

    const {key, value} = dataset[i];
    distribution[gid].store.put(value, key, (e) => {
      if (hasRealError(e)) {
        callback(e);
        return;
      }
      i += 1;
      next();
    });
  }

  next();
}

function runMr(gid, keys, callback) {
  const mapper = (key, value) => {
    const words = value.split(/\s+/).filter((w) => w.length > 0);
    return words.map((word) => ({[word]: 1}));
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((sum, v) => sum + v, 0);
    return out;
  };

  distribution[gid].mr.exec({keys, map: mapper, reduce: reducer}, callback);
}

function benchmarkMrExec(gid, keys, runs, callback) {
  console.log(`running MR workflow ${runs} times`);
  const latencies = [];
  const start = performance.now();
  let i = 0;

  function next() {
    if (i >= runs) {
      const totalMs = performance.now() - start;
      printMrStats(latencies, totalMs, keys.length);
      callback(null);
      return;
    }

    const t0 = performance.now();
    runMr(gid, keys, (e, out) => {
      latencies.push(performance.now() - t0);
      if (hasRealError(e)) {
        callback(e);
        return;
      }
      if (!Array.isArray(out)) {
        callback(Error('mr output is not an array'));
        return;
      }
      i += 1;
      next();
    });
  }

  next();
}

function shutdownAndExit(code) {
  stopNodes(() => {
    if (globalThis.distribution.node.server) {
      globalThis.distribution.node.server.close();
    }
    process.exit(code);
  });
}

function failAndExit(stage, e) {
  console.error(`benchmark failed at ${stage}:`, e);
  shutdownAndExit(1);
}

console.log('starting local M5 characterization...');
console.log(`nodes: ${NODES.map((n) => `${n.ip}:${n.port}`).join(', ')}`);
console.log(`dataset: ${DOC_COUNT} docs x ${WORDS_PER_DOC} words/doc`);
console.log(`mr runs: ${MR_RUNS}`);

distribution.node.start((startErr) => {
  if (hasRealError(startErr)) {
    failAndExit('node.start', startErr);
    return;
  }

  stopNodes(() => {
    spawnNodes(() => {
      setupGroup(GID, (groupErr) => {
        if (hasRealError(groupErr)) {
          failAndExit('setupGroup', groupErr);
          return;
        }

        const dataset = generateDataset(DOC_COUNT, WORDS_PER_DOC);
        const keys = dataset.map((d) => d.key);

        loadDataset(GID, dataset, (loadErr) => {
          if (hasRealError(loadErr)) {
            failAndExit('loadDataset', loadErr);
            return;
          }

          benchmarkMrExec(GID, keys, MR_RUNS, (mrErr) => {
            if (hasRealError(mrErr)) {
              failAndExit('benchmarkMrExec', mrErr);
              return;
            }

            console.log('done!');
            shutdownAndExit(0);
          });
        });
      });
    });
  });
});
