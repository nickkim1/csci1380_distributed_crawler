#!/usr/bin/env node
// @ts-nocheck

/**
 * End-to-end M6 books pipeline demo:
 * 1) start cluster
 * 2) crawl seed URLs
 * 3) build distributed n-gram index
 * 4) run ranked query
 *
 * Usage:
 *   node scripts/m6_e2e_books.js
 *   node scripts/m6_e2e_books.js --query "alice wonderland" --maxPages 80
 *   node scripts/m6_e2e_books.js --seed https://cs.brown.edu/courses/csci1380/sandbox/2/
 */

const distributionFactory = require("../distribution.js");
const distribution = distributionFactory();
const id = distribution.util.id;

const argv = process.argv.slice(2);

/** @param {string} name */
function readFlag(name) {
  const index = argv.indexOf(`--${name}`);
  if (index === -1) {
    return null;
  }
  return argv[index + 1] || null;
}

/** @param {string} name @param {number} fallback */
function readNumber(name, fallback) {
  const value = readFlag(name);
  if (!value) {
    return fallback;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

const seed =
  readFlag("seed") ||
  "https://cs.brown.edu/courses/csci1380/sandbox/2/";
const queryText = readFlag("query") || "read book english";
const maxDepth = readNumber("maxDepth", 2);
const maxPages = readNumber("maxPages", 60);
const gid = readFlag("gid") || "books";

const workers = [
  { ip: "127.0.0.1", port: 7661 },
  { ip: "127.0.0.1", port: 7662 },
  { ip: "127.0.0.1", port: 7663 },
];

/** @param {(err?: Error|null)=>void} done */
function startCluster(done) {
  distribution.node.start((startError) => {
    if (startError) {
      done(startError);
      return;
    }

    let i = 0;
    const spawnNext = () => {
      if (i >= workers.length) {
        return buildGroup(done);
      }
      const node = workers[i++];
      distribution.local.status.spawn(node, (spawnError) => {
        if (spawnError) {
          done(spawnError);
          return;
        }
        spawnNext();
      });
    };

    spawnNext();
  });
}

/** @param {(err?: Error|null)=>void} done */
function buildGroup(done) {
  const group = {};
  workers.forEach((node) => {
    group[id.getSID(node)] = node;
  });

  distribution.local.groups.put({ gid }, group, (localPutError) => {
    if (localPutError) {
      done(localPutError);
      return;
    }

    distribution[gid].groups.put({ gid }, group, (allPutError) => {
      if (allPutError && Object.keys(allPutError).length) {
        done(new Error(`group put failed: ${JSON.stringify(allPutError)}`));
        return;
      }
      done(null);
    });
  });
}

/** @param {(err?: Error|null)=>void} done */
function shutdown(done) {
  const stopRemote = (idx) => {
    if (idx >= workers.length) {
      if (distribution.node.server) {
        distribution.node.server.close();
      }
      done(null);
      return;
    }

    distribution.local.comm.send(
      [],
      { node: workers[idx], service: "status", method: "stop" },
      () => stopRemote(idx + 1),
    );
  };

  stopRemote(0);
}

function main() {
  const startedAt = Date.now();

  startCluster((clusterError) => {
    if (clusterError) {
      console.error("[m6-e2e] cluster startup failed:", clusterError);
      process.exitCode = 1;
      return;
    }

    const crawlStart = Date.now();
    distribution[gid].crawler.exec(
      {
        gid,
        seedURLs: [seed],
        maxDepth,
        maxPages,
      },
      (crawlError, crawlResult) => {
        if (crawlError) {
          console.error("[m6-e2e] crawl failed:", crawlError);
          return shutdown(() => {
            process.exitCode = 1;
          });
        }

        const indexStart = Date.now();
        distribution[gid].indexer.exec(
          {
            gid,
            indexGid: gid,
            crawlID: crawlResult.crawlID,
          },
          (indexError, indexResult) => {
            if (indexError) {
              console.error("[m6-e2e] index failed:", indexError);
              return shutdown(() => {
                process.exitCode = 1;
              });
            }

            const queryStart = Date.now();
            distribution[gid].query.exec(
              {
                gid,
                indexGid: gid,
                text: queryText,
                limit: 10,
                explain: true,
              },
              (queryError, queryResult) => {
                if (queryError) {
                  console.error("[m6-e2e] query failed:", queryError);
                  return shutdown(() => {
                    process.exitCode = 1;
                  });
                }

                const report = {
                  gid,
                  seed,
                  query: queryText,
                  crawlID: crawlResult.crawlID,
                  timingMs: {
                    crawl: Date.now() - crawlStart,
                    index: Date.now() - indexStart,
                    query: Date.now() - queryStart,
                    total: Date.now() - startedAt,
                  },
                  crawlResult,
                  indexedTerms: Array.isArray(indexResult) ? indexResult.length : 0,
                  topResults: Array.isArray(queryResult)
                    ? queryResult.slice(0, 10)
                    : [],
                };

                console.log(JSON.stringify(report, null, 2));

                shutdown(() => {
                  process.exitCode = 0;
                });
              },
            );
          },
        );
      },
    );
  });
}

main();
