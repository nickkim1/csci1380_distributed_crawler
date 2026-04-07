require('../../distribution.js')();
const distribution = globalThis.distribution;
const util = distribution.util;
const id = distribution.util.id;

test('(5 pts) (scenario) use the local store', (done) => {
  /*
      Use the distributed store to put a key-value pair.
      Make sure to run the check() function at the last callback of your solution.
  */
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbspsg';

  distribution.local.store.put(user, key, check);

  function check() {
    distribution.local.store.get(key, (e, v) => {
      try {
        expect(v).toEqual(user);
        done();
      } catch (error) {
        done(error);
      }
    });
  }
});


test('(5 pts) (scenario) two keys map to the same node', () => {
  /*

        Identify two keys that consistentHash maps to the same node. You will
        likely need to try a few (but not many) keys. What can you conclude
        about using consistentHash for a small number of keys.

    */
  const nodeIds = [
    util.id.getNID({ip: '192.168.0.1', port: 8000}),
    util.id.getNID({ip: '192.168.0.2', port: 8000}),
    util.id.getNID({ip: '192.168.0.3', port: 8000}),
    util.id.getNID({ip: '192.168.0.4', port: 8000}),
    util.id.getNID({ip: '192.168.0.5', port: 8000}),
  ];
  const key1 = nodeIds[0];
  const key2 = nodeIds[1];


  const kid1 = util.id.getID(key1);
  const kid2 = util.id.getID(key2);

  const key1Node = util.id.consistentHash(kid1, nodeIds);
  const key2Node = util.id.consistentHash(kid2, nodeIds);

  expect(key1Node).toEqual(key2Node);
});

test('(5 pts) (scenario) hash functions return the same node', () => {
  /*

        Identify a key for which two hash functions agree about its placement.
        You will likely need to try a few (but not many) keys.

    */

  // Feel free to change the nodes (both their number and configuration)
  const nodeIds = [
    util.id.getNID({ip: '192.168.0.1', port: 8000}),
    util.id.getNID({ip: '192.168.0.2', port: 8000}),
    util.id.getNID({ip: '192.168.0.3', port: 8000}),
    util.id.getNID({ip: '192.168.0.4', port: 8000}),
  ];

  const key = nodeIds[0];

  const kid = util.id.getID(key);

  const a = util.id.consistentHash(kid, nodeIds); // You can also experiment with other hash functions
  const b = util.id.rendezvousHash(kid, nodeIds); // Pick one of the other hash functions

  expect(a).toEqual(b);
});

const n1 = {ip: '127.0.0.1', port: 9001};
const n2 = {ip: '127.0.0.1', port: 9002};
const n3 = {ip: '127.0.0.1', port: 9003};
const n4 = {ip: '127.0.0.1', port: 9004};
const n5 = {ip: '127.0.0.1', port: 9005};
const n6 = {ip: '127.0.0.1', port: 9006};

test('(5 pts) (scenario) use mem.reconf', (done) => {
  /*
  In this scenario, you will use the `mem.reconf` method to reconfigure the placement of items in a group of nodes.
  You will create a group of nodes and place items in them.
  Then, you will remove a node from the group and call `mem.reconf` to place the items in the remaining nodes.
  Finally, you will check if the items are in the right place.
  */

  // Create a group with any number of nodes
  const mygroupGroup = {};
  // Add more nodes to the group...
  mygroupGroup[id.getSID(n1)] = n1;
  mygroupGroup[id.getSID(n2)] = n2;
  // TODO: 2 nodes makes this trivial but do i care

  // Create a set of items and corresponding keys...
  const keysAndItems = [
    {key: 'a', item: {first: 'Josiah', last: 'Carberry'}},
    {key: 'b', item: {first: 'Josia', last: 'Carberr'}},
    {key: 'c', item: {first: 'Josi', last: 'Carber'}},
  ];

  // Experiment with different hash functions...
  const config = {gid: 'mygroup', hash: id.rendezvousHash};

  distribution.local.groups.put(config, mygroupGroup, (e, v) => {
    // Now, place each one of the items you made inside the group...
    distribution.mygroup.mem.put(keysAndItems[0].item, keysAndItems[0].key, (e, v) => {
      distribution.mygroup.mem.put(keysAndItems[1].item, keysAndItems[1].key, (e, v) => {
        distribution.mygroup.mem.put(keysAndItems[2].item, keysAndItems[2].key, (e, v) => {
          // We need to pass a copy of the group's
          // nodes before the changes to reconf()
          const groupCopy = {...mygroupGroup};

          // Remove a node from the group...
          const toRemove = n2;
          distribution.local.groups.rem('mygroup', id.getSID(toRemove), (e, v) => {
            // We call `reconf()` on the distributed mem service. This will place the items in the remaining group nodes...
            distribution.mygroup.mem.reconf(groupCopy, (e, v) => {
              // Fill out the `checkPlacement` function (defined below) based on how you think the items will have been placed after the reconfiguration...
              checkPlacement(e, v);
            });
          });
        });
      });
    });
  });

  // This function will be called after we put items in nodes
  // Send the right messages to the right nodes to check if the items are in the right place...
  const checkPlacement = (e, v) => {
    const messages = [
      [{key: keysAndItems[0].key, gid: 'mygroup'}],
      [{key: keysAndItems[1].key, gid: 'mygroup'}],
      [{key: keysAndItems[2].key, gid: 'mygroup'}],
    ];

    // Based on where you think the items should be, send the messages to the right nodes...
    const remote = {node: n1, service: 'mem', method: 'get'};
    distribution.local.comm.send(messages[0], remote, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toEqual(keysAndItems[0].item);
      } catch (error) {
        done(error);
        return;
      }
      // Write checks for the rest of the items...
      const remote = {node: n1, service: 'mem', method: 'get'};
      distribution.local.comm.send(messages[1], remote, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(keysAndItems[1].item);
        } catch (error) {
          done(error);
          return;
        }
        const remote = {node: n1, service: 'mem', method: 'get'};
        distribution.local.comm.send(messages[2], remote, (e, v) => {
          try {
            expect(e).toBeFalsy();
            expect(v).toEqual(keysAndItems[2].item);
          } catch (error) {
            done(error);
            return;
          }
          done(); // Only call `done()` once all checks are written
        });
      });
    });
  };
});

test('(5 pts) (scenario) redistribute keys and values among nodes', (done) => {
  /*
    This scenario simulates the "Shuffle" phase of MapReduce with multiple keys.

    Setup:
    - n1 has local results: { 'jcarb': 'one', 'lc': 'three' }
    - n2 has local results: { 'jcarb': 'two' }

    Goal:
    - 'jcarb' should be aggregated to ['one', 'two']
    - 'lc' should be aggregated to ['three']

    Your Task:
    1. Fetch the local values from n1 and n2.
    2. For every key-value pair found, use `shuffleGroup.store.append` to
       send it to the correct destination in the distributed system.

    This forces you to trust the hashing mechanism: you don't know where
    'jcarb' or 'lc' will end up, but `append` will route them correctly.
  */

  const shuffleGroup = {};
  shuffleGroup[id.getSID(n1)] = n1;
  shuffleGroup[id.getSID(n2)] = n2;
  shuffleGroup[id.getSID(n3)] = n3;

  // The "map output" data scattered across nodes
  const n1Data = {'jcarb': 'one', 'lc': 'three'};
  const n2Data = {'jcarb': 'two'};

  distribution.local.groups.put('shuffleGroup', shuffleGroup, (e, v) => {
    // Helper to seed local storage (simulating map output)
    const seed = (node, data, callback) => {
      const entries = Object.entries(data);
      let pending = entries.length;
      if (pending === 0) return callback();

      entries.forEach(([k, v]) => {
        const remote = {node: node, service: 'store', method: 'put'};
        const config = {key: k, gid: 'local'};
        distribution.local.comm.send([v, config], remote, (e, v) => {
          if (--pending === 0) {
            return callback();
          }
        });
      });
    };

    // Seed n1 and n2, then run solution
    seed(n1, n1Data, () => {
      seed(n2, n2Data, () => {
        runSolution();
      });
    });
  });

  const runSolution = () => {
    // Helper to process a single node's data
    const processNode = (node, dataToProcess, callback) => {
      const entries = Object.entries(dataToProcess);
      entries.forEach(([key, val]) => distribution.shuffleGroup.store.append(val, key, callback));
    };

    // Process n1's data, then n2's data, and finlly check the results
    processNode(n1, n1Data, () => {
      processNode(n2, n2Data, () => {
        check();
      });
    });
  };

  const check = () => {
    // Check 'jcarb' aggregation
    distribution.shuffleGroup.store.get('jcarb', (e, v) => {
      try {
        expect(e).toBeFalsy();
        // What do you expect the value to be?

        // Check 'lc' aggregation
        distribution.shuffleGroup.store.get('lc', (e, v) => {
          expect(e).toBeFalsy();
          // What do you expect the value to be?
          done();
        });
      } catch (error) {
        done(error);
      }
    });
  };
});

beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
              startNodes();
            });
          });
        });
      });
    });
  });

  const startNodes = () => {
    // Now, start the nodes listening node
    distribution.node.start(() => {
      // Start the nodes
      distribution.local.status.spawn(n1, (e, v) => {
        distribution.local.status.spawn(n2, (e, v) => {
          distribution.local.status.spawn(n3, (e, v) => {
            distribution.local.status.spawn(n4, (e, v) => {
              distribution.local.status.spawn(n5, (e, v) => {
                distribution.local.status.spawn(n6, (e, v) => {
                  done();
                });
              });
            });
          });
        });
      });
    });
  };
});


afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
              if (globalThis.distribution.node.server) {
                globalThis.distribution.node.server.close();
              }
              done();
            });
          });
        });
      });
    });
  });
});
