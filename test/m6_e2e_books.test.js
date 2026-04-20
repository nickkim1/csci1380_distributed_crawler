require("../distribution.js")();
require("./helpers/sync-guard");

const distribution = globalThis.distribution;
const id = distribution.util.id;

jest.spyOn(process, "exit").mockImplementation(() => {});
jest.setTimeout(60000);

const gid = "books_e2e_test";
const indexGid = `index_${gid}`;
const workers = [
  { ip: "127.0.0.1", port: 8861 },
  { ip: "127.0.0.1", port: 8862 },
  { ip: "127.0.0.1", port: 8863 },
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

test("(integration) books pipeline crawls and returns ranked query results", (done) => {
  const seed = "https://cs.brown.edu/courses/csci1380/sandbox/3/";

  distribution[gid].crawler.exec(
    {
      urls: [seed],
      indexGid,
      maxDepth: 1,
      maxPages: 40,
    },
    (crawlErr, crawlStats) => {
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
      } catch (assertErr) {
        return done(assertErr);
      }

      distribution[gid].query.exec(
        {
          indexGid: crawlStats.indexGid,
          query: "book summary",
          limit: 10,
        },
        (queryErr, queryOut) => {
          if (hasError(queryErr)) {
            return done(queryErr);
          }

          try {
            expect(queryOut).toBeTruthy();
            expect(Array.isArray(queryOut.results)).toBe(true);
            expect(queryOut.results.length).toBeGreaterThan(0);

            queryOut.results.forEach((row) => {
              expect(typeof row.url).toBe("string");
              expect(row.url.length).toBeGreaterThan(0);
              expect(Number.isFinite(Number(row.score))).toBe(true);
            });

            done();
          } catch (assertErr) {
            done(assertErr);
          }
        },
      );
    },
  );
});
