/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
require('../helpers/sync-guard');
const id = distribution.util.id;

jest.spyOn(process, 'exit').mockImplementation((n) => { });

const mygroupGroup = {};

const n1 = {ip: '127.0.0.1', port: 8000};
const n2 = {ip: '127.0.0.1', port: 8001};
const n3 = {ip: '127.0.0.1', port: 8002};

// local.groups.put dynamically creates distribution[gid] with services
test('(1 pts) student test', (done) => {
  const g = {
    [id.getSID(n1)]: n1,
    [id.getSID(n2)]: n2,
  };

  distribution.local.groups.put('dyngroup', g, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(distribution.dyngroup).toBeDefined();
      expect(distribution.dyngroup.comm).toBeDefined();
      expect(distribution.dyngroup.status).toBeDefined();
      expect(distribution.dyngroup.groups).toBeDefined();
      expect(distribution.dyngroup.routes).toBeDefined();
      done();
    } catch (error) {
      done(error);
    }
  });
});

// all.comm.send via local.comm.send using the gid field on a remote node
test('(1 pts) student test', (done) => {
  const mygroupConfig = {gid: 'mygroup'};
  const remote = {node: n1, service: 'groups', method: 'put'};
  distribution.local.comm.send([mygroupConfig, mygroupGroup], remote, (e, v) => {
    try {
      expect(e).toBeFalsy();
    } catch (error) {
      done(error);
      return;
    }
    const statusRemote = {node: n1, gid: 'mygroup', service: 'status', method: 'get'};
    distribution.local.comm.send(['sid'], statusRemote, (e, v) => {
      try {
        expect(e).toEqual({});
        const sids = Object.values(v);
        expect(sids.length).toBeGreaterThan(0);
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

// service registered on a subgroup is accessible on its members but not on non-members
test('(1 pts) student test', (done) => {
  const subgroupNodes = {
    [id.getSID(n1)]: n1,
    [id.getSID(n2)]: n2,
  };
  distribution.local.groups.put({gid: 'subgroup'}, subgroupNodes, (e, v) => {
    const isolatedService = {ping: (cb) => cb(null, 'pong')};
    distribution.subgroup.routes.put(isolatedService, 'isolated', (e, v) => {
      try {
        expect(e).toEqual({});
      } catch (error) {
        done(error);
        return;
      }
      const r1 = {node: n1, service: 'routes', method: 'get'};
      distribution.local.comm.send(['isolated'], r1, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBeDefined();
        } catch (error) {
          done(error);
          return;
        }
        const r3 = {node: n3, service: 'routes', method: 'get'};
        distribution.local.comm.send(['isolated'], r3, (e, v) => {
          try {
            expect(e).toBeDefined();
            expect(e).toBeInstanceOf(Error);
            done();
          } catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

// all.routes.put a custom service, then all.comm.send invokes it on every node
test('(1 pts) student test', (done) => {
  const echoService = {
    echo: (msg, callback) => {
      callback(null, 'echo:' + msg);
    },
  };
  distribution.mygroup.routes.put(echoService, 'echoSvc', (e, v) => {
    try {
      expect(e).toEqual({});
    } catch (error) {
      done(error);
      return;
    }
    distribution.mygroup.comm.send(
        ['hi'], {service: 'echoSvc', method: 'echo'}, (e, v) => {
          try {
            expect(e).toEqual({});
            const sids = Object.keys(v);
            expect(sids.length).toBe(Object.keys(mygroupGroup).length);
            sids.forEach((sid) => {
              expect(v[sid]).toBe('echo:hi');
            });
            done();
          } catch (error) {
            done(error);
          }
        });
  });
});

// all.groups.put propagates group and then all.groups.del removes it on all nodes
test('(1 pts) student test', (done) => {
  const g = {
    'aaa11': {ip: '127.0.0.1', port: 7070},
    'bbb22': {ip: '127.0.0.1', port: 7071},
  };
  distribution.mygroup.groups.put('browncs', g, (e, v) => {
    try {
      expect(e).toEqual({});
      Object.keys(mygroupGroup).forEach((sid) => {
        expect(v[sid]).toEqual(g);
      });
    } catch (error) {
      done(error);
      return;
    }
    distribution.mygroup.groups.del('browncs', (e, v) => {
      try {
        expect(e).toEqual({});
        Object.keys(mygroupGroup).forEach((sid) => {
          expect(v[sid]).toEqual(g);
        });
      } catch (error) {
        done(error);
        return;
      }
      distribution.mygroup.groups.get('browncs', (e, v) => {
        try {
          Object.keys(mygroupGroup).forEach((sid) => {
            expect(e[sid]).toBeDefined();
            expect(e[sid]).toBeInstanceOf(Error);
          });
          expect(v).toEqual({});
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });
});

/* Test infrastructure */

beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
      });
    });
  });

  mygroupGroup[id.getSID(n1)] = n1;
  mygroupGroup[id.getSID(n2)] = n2;
  mygroupGroup[id.getSID(n3)] = n3;

  // Now, start the base listening node
  distribution.node.start((e) => {
    if (e) {
      done(e);
      return;
    }

    const groupInstantiation = () => {
      const mygroupConfig = {gid: 'mygroup'};

      // Create some groups
      distribution.local.groups
          .put(mygroupConfig, mygroupGroup, (e, v) => {
            done();
          });
    };

    // Start the nodes
    distribution.local.status.spawn(n1, (e, v) => {
      if (e) {
        done(e);
        return;
      }
      distribution.local.status.spawn(n2, (e, v) => {
        if (e) {
          done(e);
          return;
        }
        distribution.local.status.spawn(n3, (e, v) => {
          if (e) {
            done(e);
            return;
          }
          groupInstantiation();
        });
      });
    });
  });
});

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        if (globalThis.distribution.node.server) {
          globalThis.distribution.node.server.close();
        }
        done();
      });
    });
  });
});
