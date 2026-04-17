require("../distribution.js")();
require("./helpers/sync-guard");

const distribution = globalThis.distribution;
const id = distribution.util.id;

jest.spyOn(process, "exit").mockImplementation(() => {});
jest.setTimeout(180000);

const group = {};
const GID = "gutenbergit";

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

beforeAll((done) => {
  distribution.node.start((startErr) => {
    if (hasError(startErr)) {
      return done(startErr);
    }

    const localNode = distribution.node.config;
    group[id.getSID(localNode)] = localNode;

    const config = { gid: GID };
    distribution.local.groups.put(config, group, (localErr) => {
      if (hasError(localErr)) {
        return done(localErr);
      }
      distribution[GID].groups.put(config, group, (groupErr) => {
        if (hasError(groupErr)) {
          return done(groupErr);
        }
        done();
      });
    });
  });
});

afterAll((done) => {
  if (globalThis.distribution.node.server) {
    globalThis.distribution.node.server.close(() => done());
    return;
  }
  done();
});

test("(integration) crawler -> shard index files -> query on gutenberg seed", (done) => {
  const seedURL = "https://atlas.cs.brown.edu/data/gutenberg/";

  distribution[GID].crawler.exec({ urls: [seedURL] }, (crawlErr, crawlOut) => {
    if (hasError(crawlErr)) {
      return done(crawlErr);
    }

    try {
      expect(crawlOut).toBeTruthy();
      expect(crawlOut.docs).toBeGreaterThan(0);
      expect(crawlOut.terms).toBeGreaterThan(0);
      expect(crawlOut.indexGid).toBeTruthy();
      expect(crawlOut.files).toBeTruthy();
      expect(Object.keys(crawlOut.files).length).toBeGreaterThan(0);
    } catch (assertErr) {
      return done(assertErr);
    }

    // Confirm shard file is readable and contains term->postings entries.
    const [sid, fileKey] = Object.entries(crawlOut.files)[0];
    const node = group[sid];
    distribution.local.comm.send(
      [{ key: fileKey, gid: crawlOut.indexGid }],
      { node, service: "store", method: "get", gid: "local" },
      (shardErr, shard) => {
        if (hasError(shardErr)) {
          return done(shardErr);
        }

        try {
          expect(shard).toBeTruthy();
          expect(typeof shard).toBe("object");
          expect(Object.keys(shard).length).toBeGreaterThan(0);
        } catch (assertErr) {
          return done(assertErr);
        }

        distribution[GID].query.exec(
          {
            query: "gutenberg project",
            indexGid: crawlOut.indexGid,
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

              // The query should return URLs from the crawled site.
              const urls = queryOut.results.map((r) => r.url);
              expect(
                urls.some((u) =>
                  u.includes("atlas.cs.brown.edu/data/gutenberg"),
                ),
              ).toBe(true);
              done();
            } catch (assertErr) {
              done(assertErr);
            }
          },
        );
      },
    );
  });
});
