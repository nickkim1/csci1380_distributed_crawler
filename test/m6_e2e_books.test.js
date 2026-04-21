require("../distribution.js")();
require("./helpers/sync-guard");

const distribution = globalThis.distribution;
const id = distribution.util.id;

jest.spyOn(process, "exit").mockImplementation(() => {});
jest.setTimeout(300000);

const gid = "books_e2e_test";
const indexGid = `index_${gid}`;
const workers = [
  { ip: "127.0.0.1", port: 8861 },
  { ip: "127.0.0.1", port: 8862 },
  { ip: "127.0.0.1", port: 8863 },
];

const corpora = [
  {
    name: "sandbox2",
    seed: "https://cs.brown.edu/courses/csci1380/sandbox/2/",
    queries: ["absenc", "camera improv"],
    maxDepth: 1,
    maxPages: 30,
    expectedHostIncludes: "cs.brown.edu",
  },
  {
    name: "sandbox3",
    seed: "https://cs.brown.edu/courses/csci1380/sandbox/3/",
    queries: ["wright", "book summary"],
    maxDepth: 1,
    maxPages: 40,
    expectedHostIncludes: "cs.brown.edu",
  },
  {
    name: "gutenberg",
    seed: "https://atlas.cs.brown.edu/data/gutenberg/",
    queries: ["book", "author"],
    maxDepth: 1,
    maxPages: 20,
    expectedHostIncludes: "atlas.cs.brown.edu",
  },
];

function hasError(err) {
  if (!err) {
    return false;
  }
  if (err instanceof Error) {
    return true;
  }
  if (typeof err === "object") {
    return Object.keys(err).length > 0;
  }
  return true;
}

function startCluster(done) {
  distribution.node.start((startErr) => {
    if (hasError(startErr)) {
      return done(startErr);
    }

    let i = 0;
    const spawnNext = () => {
      if (i >= workers.length) {
        return buildGroup(done);
      }
      const node = workers[i++];
      distribution.local.status.spawn(node, (spawnErr) => {
        if (hasError(spawnErr)) {
          return done(spawnErr);
        }
        spawnNext();
      });
    };

    spawnNext();
  });
}

function buildGroup(done) {
  const group = {};
  workers.forEach((node) => {
    group[id.getSID(node)] = node;
  });

  distribution.local.groups.put({ gid }, group, (localErr) => {
    if (hasError(localErr)) {
      return done(localErr);
    }

    distribution[gid].groups.put({ gid }, group, (groupErr) => {
      if (hasError(groupErr)) {
        return done(groupErr);
      }
      done();
    });
  });
}

function stopCluster(done) {
  let i = 0;
  const stopNext = () => {
    if (i >= workers.length) {
      if (distribution.node.server) {
        return distribution.node.server.close(() => done());
      }
      return done();
    }

    distribution.local.comm.send(
      [],
      { node: workers[i++], service: "status", method: "stop" },
      () => stopNext(),
    );
  };

  stopNext();
}

beforeAll((done) => {
  startCluster(done);
});

afterAll((done) => {
  stopCluster(done);
});

function crawlCorpus(corpus, cb) {
  distribution[gid].crawler.exec(
    {
      urls: [corpus.seed],
      indexGid: `${indexGid}_${corpus.name}`,
      maxDepth: corpus.maxDepth,
      maxPages: corpus.maxPages,
    },
    cb,
  );
}

function queryIndex(indexName, query, cb) {
  distribution[gid].query.exec(
    {
      indexGid: indexName,
      query,
      limit: 10,
    },
    cb,
  );
}

describe("(integration) paired-corpus correctness checks", () => {
  test.each(corpora)(
    "runs shared crawl+query correctness assertions for %s",
    (corpus, done) => {
      crawlCorpus(corpus, (crawlErr, crawlStats) => {
        if (hasError(crawlErr)) {
          return done(crawlErr);
        }

        try {
          expect(crawlStats).toBeTruthy();
          expect(crawlStats.indexGid).toBeTruthy();
          expect(Number(crawlStats.docs || 0)).toBeGreaterThan(0);
          expect(Number(crawlStats.terms || 0)).toBeGreaterThan(0);
          expect(crawlStats.files).toBeTruthy();
          expect(Object.keys(crawlStats.files).length).toBeGreaterThan(0);

          const crawlPhase = crawlStats.crawlStats || {};
          expect(Number(crawlPhase.pagesFetched || 0)).toBeGreaterThan(0);
        } catch (assertErr) {
          return done(assertErr);
        }

        let i = 0;
        const runNextQuery = () => {
          if (i >= corpus.queries.length) {
            return done();
          }

          const q = corpus.queries[i++];
          queryIndex(crawlStats.indexGid, q, (queryErr, queryOut) => {
            if (hasError(queryErr)) {
              return done(queryErr);
            }

            try {
              expect(queryOut).toBeTruthy();
              expect(Array.isArray(queryOut.results)).toBe(true);
              expect(queryOut.results.length).toBeGreaterThan(0);

              const top = queryOut.results[0];
              expect(typeof top.url).toBe("string");
              expect(top.url).toContain(corpus.expectedHostIncludes);

              let previousScore = Number.POSITIVE_INFINITY;
              queryOut.results.forEach((row) => {
                const score = Number(row.score);
                expect(typeof row.url).toBe("string");
                expect(row.url.length).toBeGreaterThan(0);
                expect(Number.isFinite(score)).toBe(true);
                expect(score).toBeLessThanOrEqual(previousScore);
                previousScore = score;
              });

              runNextQuery();
            } catch (assertErr) {
              done(assertErr);
            }
          });
        };

        runNextQuery();
      });
    },
  );
});
