require('../distribution.js')();
require('./helpers/sync-guard.js');

const distribution = globalThis.distribution;
const id = distribution.util.id;

const {
  crawlMap,
  crawlReduce,
  runCrawlStage,
  runPipeline,
  queryIndex,
} = require('../m6/pipeline.js');

const n1 = {ip: '127.0.0.1', port: 7310};
const n2 = {ip: '127.0.0.1', port: 7311};
const n3 = {ip: '127.0.0.1', port: 7312};

const crawlGroup = {};
const ridxGroup = {};
const rlgGroup = {};

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

function setupGroup(gid, group, callback) {
  const config = {gid};
  distribution.local.groups.put(config, group, (e) => {
    if (hasRealError(e)) {
      callback(e);
      return;
    }

    distribution[gid].groups.put(config, group, (e2) => {
      callback(e2);
    });
  });
}

test('m6 crawl stage stores doc payloads', (done) => {
  const seeds = [
    {
      key: 'https://example.org/start-a',
      value: {
        url: 'https://example.org/start-a',
        html: '<html><body><a href="https://example.org/book-a.txt">A</a><a href="https://example.org/page-b">B</a><p>Gutenberg crawler example text.</p></body></html>',
      },
    },
    {
      key: 'https://example.org/start-b',
      value: {
        url: 'https://example.org/start-b',
        html: '<html><body><a href="https://example.org/book-a.txt">Dup</a><p>Distributed index test content.</p></body></html>',
      },
    },
  ];

  runCrawlStage(distribution, {
    crawlGid: 'crawl',
    ridxGid: 'ridx',
    seedRecords: seeds,
  }, (e, stats) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    try {
      expect(stats.discoveredUrls).toBeGreaterThan(0);
      expect(stats.indexedDocs).toBeGreaterThan(0);
      expect(Array.isArray(stats.docRecords)).toBe(true);
      expect(stats.docRecords.length).toBeGreaterThan(0);
    } catch (assertErr) {
      done(assertErr);
      return;
    }

    done();
  });
});

test('m6 end-to-end pipeline answers query', (done) => {
  const seeds = [
    {
      key: 'https://example.org/book-1',
      value: {
        url: 'https://example.org/book-1',
        html: '<html><body><p>Project gutenberg distributed crawler project gutenberg systems.</p></body></html>',
      },
    },
    {
      key: 'https://example.org/book-2',
      value: {
        url: 'https://example.org/book-2',
        html: '<html><body><p>Search engine index and retrieval over distributed nodes.</p></body></html>',
      },
    },
  ];

  runPipeline(distribution, {
    crawlGid: 'crawl',
    ridxGid: 'ridx',
    rlgGid: 'rlg',
    seedRecords: seeds,
  }, (e, stats) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    try {
      expect(stats.crawl.indexedDocs).toBeGreaterThan(0);
      expect(stats.index.terms).toBeGreaterThan(0);
    } catch (assertErr) {
      done(assertErr);
      return;
    }

    queryIndex(distribution, {rlgGid: 'rlg', topK: 5}, 'project gutenberg distributed', (queryErr, ranked) => {
      if (hasRealError(queryErr)) {
        done(queryErr);
        return;
      }

      try {
        expect(Array.isArray(ranked)).toBe(true);
        expect(ranked.length).toBeGreaterThan(0);
        expect(ranked[0].score).toBeGreaterThan(0);
        done();
      } catch (assertErr) {
        done(assertErr);
      }
    });
  });
});

test('m6 crawl stage keeps URL records when seed html is missing', (done) => {
  const seeds = [
    {
      key: 'https://example.org/no-html-seed',
      value: {
        url: 'https://example.org/no-html-seed',
      },
    },
  ];

  runCrawlStage(distribution, {
    crawlGid: 'crawl',
    ridxGid: 'ridx',
    seedRecords: seeds,
  }, (e, stats) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    try {
      expect(stats.discoveredUrls).toBeGreaterThan(0);
      expect(stats.indexedDocs).toBe(0);
    } catch (assertErr) {
      done(assertErr);
      return;
    }

    done();
  });
});

test('m6 crawl stage tolerates malformed html without mapper throw', (done) => {
  const seeds = [
    {
      key: 'https://example.org/malformed-html',
      value: {
        url: 'https://example.org/malformed-html',
        html: '<html><body><a href="https://example.org/linked">broken<p>unterminated',
      },
    },
  ];

  runCrawlStage(distribution, {
    crawlGid: 'crawl',
    ridxGid: 'ridx',
    seedRecords: seeds,
  }, (e, stats) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    try {
      expect(stats.discoveredUrls).toBeGreaterThan(0);
      expect(stats.indexedDocs).toBeGreaterThanOrEqual(0);
    } catch (assertErr) {
      done(assertErr);
      return;
    }

    done();
  });
});

test('m6 query returns empty list when terms are absent', (done) => {
  const seeds = [
    {
      key: 'https://example.org/query-absent',
      value: {
        url: 'https://example.org/query-absent',
        html: '<html><body><p>distributed systems crawler index.</p></body></html>',
      },
    },
  ];

  runPipeline(distribution, {
    crawlGid: 'crawl',
    ridxGid: 'ridx',
    rlgGid: 'rlg',
    seedRecords: seeds,
  }, (e) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    queryIndex(distribution, {rlgGid: 'rlg', topK: 5}, 'qzvwxplmnotfound termneverindexed', (queryErr, ranked) => {
      if (hasRealError(queryErr)) {
        done(queryErr);
        return;
      }

      try {
        expect(Array.isArray(ranked)).toBe(true);
        expect(ranked).toHaveLength(0);
      } catch (assertErr) {
        done(assertErr);
        return;
      }

      done();
    });
  });
});

test('m6 crawl stage dedupes duplicate discovered URLs before persist', (done) => {
  const seeds = [
    {
      key: 'https://example.org/dedupe-a',
      value: {
        url: 'https://example.org/dedupe-a',
        html: '<html><body><a href="https://example.org/shared">x</a><a href="https://example.org/shared">y</a><p>a text</p></body></html>',
      },
    },
    {
      key: 'https://example.org/dedupe-b',
      value: {
        url: 'https://example.org/dedupe-b',
        html: '<html><body><a href="https://example.org/shared">z</a><p>b text</p></body></html>',
      },
    },
  ];

  runCrawlStage(distribution, {
    crawlGid: 'crawl',
    ridxGid: 'ridx',
    seedRecords: seeds,
  }, (e, stats) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    try {
      expect(stats.discoveredUrls).toBe(3);
      expect(stats.indexedDocs).toBe(2);
    } catch (assertErr) {
      done(assertErr);
      return;
    }

    done();
  });
});

test('m6 MR execution keeps crawl mapper/reducer serialization-safe', (done) => {
  const seedKey = 'https://example.org/serialization-guard';
  const seedValue = {
    url: seedKey,
    html: '<html><body><a href="https://example.org/serial-link">link</a><p>serialization safety text</p></body></html>',
  };

  distribution.crawl.store.put(seedValue, seedKey, (putErr) => {
    if (hasRealError(putErr)) {
      done(putErr);
      return;
    }

    distribution.crawl.mr.exec({keys: [seedKey], map: crawlMap, reduce: crawlReduce}, (e, out) => {
      if (hasRealError(e)) {
        done(e);
        return;
      }

      try {
        expect(Array.isArray(out)).toBe(true);
        expect(out.length).toBeGreaterThan(0);
      } catch (assertErr) {
        done(assertErr);
        return;
      }

      done();
    });
  });
});

beforeAll((done) => {
  crawlGroup[id.getSID(n1)] = n1;
  crawlGroup[id.getSID(n2)] = n2;
  crawlGroup[id.getSID(n3)] = n3;

  ridxGroup[id.getSID(n1)] = n1;
  ridxGroup[id.getSID(n2)] = n2;
  ridxGroup[id.getSID(n3)] = n3;

  rlgGroup[id.getSID(n1)] = n1;
  rlgGroup[id.getSID(n2)] = n2;
  rlgGroup[id.getSID(n3)] = n3;

  distribution.node.start((e) => {
    if (hasRealError(e)) {
      done(e);
      return;
    }

    distribution.local.status.spawn(n1, (s1Err) => {
      if (hasRealError(s1Err)) {
        done(s1Err);
        return;
      }

      distribution.local.status.spawn(n2, (s2Err) => {
        if (hasRealError(s2Err)) {
          done(s2Err);
          return;
        }

        distribution.local.status.spawn(n3, (s3Err) => {
          if (hasRealError(s3Err)) {
            done(s3Err);
            return;
          }

          setupGroup('crawl', crawlGroup, (crawlErr) => {
            if (hasRealError(crawlErr)) {
              done(crawlErr);
              return;
            }

            setupGroup('ridx', ridxGroup, (ridxErr) => {
              if (hasRealError(ridxErr)) {
                done(ridxErr);
                return;
              }

              setupGroup('rlg', rlgGroup, (rlgErr) => {
                if (hasRealError(rlgErr)) {
                  done(rlgErr);
                  return;
                }
                done();
              });
            });
          });
        });
      });
    });
  });
});

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, () => {
    remote.node = n2;
    distribution.local.comm.send([], remote, () => {
      remote.node = n3;
      distribution.local.comm.send([], remote, () => {
        if (globalThis.distribution.node.server) {
          globalThis.distribution.node.server.close();
        }
        done();
      });
    });
  });
});
