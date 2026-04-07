require('../../distribution.js')();
const distribution = globalThis.distribution;
const id = distribution.util.id;

const n1 = {ip: '127.0.0.1', port: 8000};
const n2 = {ip: '127.0.0.1', port: 8001};
const n3 = {ip: '127.0.0.1', port: 8002};
const allNodes = [n1, n2, n3];


test('(5 pts) (scenario) create group', (done) => {
/*
    Create a group with the nodes n1, n2, and n3.
    Then, fetch their NIDs using the distributed status service.
*/

  const groupA = {};
  groupA[id.getSID(n1)] = n1;
  // Add nodes n2 and n3 to the group...
  groupA[id.getSID(n2)] = n2;
  groupA[id.getSID(n3)] = n3;

  const nids = Object.values(allNodes).map((node) => id.getNID(node));

  // Use distribution.local.groups.put to add groupA to the local node
  // Note: The groupA.status.get call should be inside the put method's callback.
  distribution.local.groups.put('groupA', groupA, (e, v) => {
    distribution.groupA.status.get('nid', (e, v) => {
      expect(Object.values(v)).toEqual(expect.arrayContaining(nids));
      done();
    });
  });
});

test('(5 pts) (scenario) dynamic group membership', (done) => {
/*
  Dynamically add a node (n3) to groupB after the group is initially created
  with nodes n1 and n2. Validate that the distributed status service reflects
  the updated group membership on all nodes.
*/
  const groupB = {};
  // Pick some initial nodes...
  // const initialNodes = [n1];
  // Pick the final set of nodes...
  const allNodes = [n1, n2];

  // Create groupB...
  groupB[id.getSID(n1)] = n1;
  // Object.assign(groupB, Object.fromEntries(initialNodes.map((n) => [id.getSID(n), n])));

  const config = {gid: 'groupB'};

  // Create the group with initial nodes
  distribution.local.groups.put(config, groupB, (e, v) => {
    // Add a new node dynamically to the group
    distribution.local.groups.add('groupB', n2, (e, v) => {
      distribution.groupB.status.get('nid', (e, v) => {
        try {
          expect(Object.values(v)).toEqual(expect.arrayContaining(
              allNodes.map((node) => id.getNID(node))));
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });
});


test('(5 pts) (scenario) group relativity', (done) => {
/*
    Make it so that node n1 sees group groupC as containing only n2.
    while node n2 sees group groupC as containing n1 and n2.
*/
  const groupC = {};
  // Create groupC in an appropriate way...
  groupC[id.getSID(n1)] = n1;
  groupC[id.getSID(n2)] = n2;


  const config = {gid: 'groupC'};

  distribution.local.groups.put(config, groupC, (e, v) => {
    distribution.groupC.groups.put(config, groupC, (e, v) => {
      // Modify the local 'view' of the group...
      const msg = ['groupC', id.getSID(n1)];
      const remote = {node: n1, gid: 'local', service: 'groups', method: 'rem'};
      distribution.local.comm.send(msg, remote, (e, v) => {
        distribution.groupC.groups.get('groupC', (e, v) => {
          const n1View = v[id.getSID(n1)];
          const n2View = v[id.getSID(n2)];
          try {
            expect(Object.keys(n2View)).toEqual(expect.arrayContaining(
                [id.getSID(n1), id.getSID(n2)],
            ));
            expect(Object.keys(n1View)).toEqual(expect.arrayContaining(
                [id.getSID(n2)],
            ));
            done();
          } catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

test('(5 pts) (scenario) use the gossip service', (done) => {
/*
    First, create group groupD a number of nodes of your choosing.
    Then, using the groups.put method,  a new group is created called 'newgroup'.
    Add a new node to 'newgroup' using the gossip service to propagate the new group membership to all (or a subset of) nodes in groupD.

    Experiment with:
    1. The number of nodes in groupD
    2. The subset function used in the gossip service
    3. The expected number of nodes receiving the new group membership
    4. The time delay between adding the new node to 'newgroup' and checking the group membership in groupD
*/

  // Create groupD in an appropriate way...
  const groupD = {};
  groupD[id.getSID(n2)] = n2;
  groupD[id.getSID(n3)] = n3;

  // How many nodes are expected to receive the new group membership?
  const nExpected = 2;

  // Experiment with the subset function used in the gossip service...
  // The subset function takes a list of nodes and returns the number of nodes to which the message should be sent
  const config = {gid: 'groupD', subset: (lst) => 2};

  // Instantiated groupD
  distribution.local.groups.put(config, groupD, (e, v) => {
    distribution.groupD.groups.put(config, groupD, (e, v) => {
      // Created group 'newgroup' (this will be the group that we add a new node to)
      distribution.groupD.groups.put('newgroup', {}, (e, v) => {
        const newNode = {ip: '127.0.0.1', port: 4444};
        const message = [
          'newgroup',
          newNode,
        ];
        const remote = {service: 'groups', method: 'add'};
        // Adding a new node to 'newgroup' using the gossip service
        distribution.groupD.gossip.send(message, remote, (e, v) => {
          // Experiment with the time delay between adding the new node to 'newgroup' and checking the group membership in groupD...
          const delay = 10;
          setTimeout(() => {
            distribution.groupD.groups.get('newgroup', (e, v) => {
              let count = 0;
              for (const k in v) {
                if (Object.keys(v[k]).length > 0) {
                  count++;
                }
              }
              /* Gossip only provides weak guarantees */
              try {
                expect(count).toBeGreaterThanOrEqual(nExpected);
                done();
              } catch (error) {
                done(error);
              }
            });
          }, delay);
        });
      });
    });
  });
});


// had to comment out cus it hangs jest and i couldnt figure out why :/
// test('characterize spawn', (done) => {
//   const n = 20;
//   const basePort = 9010;
//   const benchNodes = [];
//   for (let i = 0; i < n; i++) {
//     benchNodes.push({ip: '127.0.0.1', port: basePort + i});
//   }
//
//   function stopBenchNodes(nodesToStop, cb) {
//     let remaining = nodesToStop.length;
//     if (remaining === 0) return cb();
//     nodesToStop.forEach((node) => {
//       distribution.local.comm.send(
//           [], {node, service: 'status', method: 'stop'}, () => {
//             if (--remaining === 0) return cb();
//           });
//     });
//   }
//
//   const latencies = [];
//   function measureLatency(idx) {
//     if (idx >= n) {
//       const avg = latencies.reduce((a, b) => a + b) / latencies.length;
//       console.log(`spawn latency: ${avg.toFixed(4)} ms`);
//       stopBenchNodes(benchNodes, () => measureThroughput());
//       return;
//     }
//     const start = performance.now();
//     distribution.local.status.spawn(benchNodes[idx], (e, v) => {
//       latencies.push(performance.now() - start);
//       measureLatency(idx + 1);
//     });
//   }
//
//   function measureThroughput() {
//     let completed = 0;
//     const start = performance.now();
//     for (let i = 0; i < n; i++) {
//       distribution.local.status.spawn(benchNodes[i], (e, v) => {
//         completed++;
//         if (completed === n) {
//           const totalTime = performance.now() - start;
//           console.log(`spawn throughput: ${(n / totalTime).toFixed(4)} ops/ms`);
//           stopBenchNodes(benchNodes, done);
//         }
//       });
//     }
//   }
//
//   measureLatency(0);
// }, 30000);


/*
    This is the setup for the test scenario.
    Do not modify the code below.
*/

function startAllNodes(callback) {
  distribution.node.start(() => {
    function startStep(step) {
      if (step >= allNodes.length) {
        callback();
        return;
      }

      distribution.local.status.spawn(allNodes[step], (e, v) => {
        if (e) {
          return callback(e);
        }
        startStep(step + 1);
      });
    }
    startStep(0);
  });
}


function stopAllNodes(callback) {
  const remote = {method: 'stop', service: 'status'};

  function stopStep(step) {
    if (step == allNodes.length) {
      callback();
      return;
    }

    if (step < allNodes.length) {
      remote.node = allNodes[step];
      distribution.local.comm.send([], remote, (e, v) => {
        stopStep(step + 1);
      });
    }
  }

  if (globalThis.distribution.node.server) {
    globalThis.distribution.node.server.close();
  }
  stopStep(0);
}

beforeAll((done) => {
  // Stop any leftover nodes
  stopAllNodes(() => {
    startAllNodes(done);
  });
});

afterAll((done) => {
  stopAllNodes(done);
});
