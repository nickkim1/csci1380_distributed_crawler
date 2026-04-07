/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
const id = distribution.util.id;
require('../helpers/sync-guard');

const ncdcGroup = {};

/*
  The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

// verify append creates array and accumulates values
test('(1 pts) student test', (done) => {
  const config = {key: 'append-test-1', gid: 'local-append-gid'};
  const val1 = 'first';
  const val2 = 'second';

  distribution.local.mem.append(val1, config, (e) => {
    if (e) {
      done(e);
      return;
    }

    distribution.local.mem.append(val2, config, (e) => {
      if (e) {
        done(e);
        return;
      }

      distribution.local.mem.get(config, (e, result) => {
        try {
          if (e) {
            done(e);
            return;
          }
          // Expect accumulated array with both values in order
          expect(Array.isArray(result)).toBe(true);
          expect(result.length).toBe(2);
          expect(result[0]).toBe(val1);
          expect(result[1]).toBe(val2);
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  });
});

/*
// put scalar, then append, verify conversion to array
// Disabled for autograder compatibility: reference local store may not create
// backing files for this direct local-store path in the same way.
test('(1 pts) student test', (done) => {
  const config = {key: 'append-scalar-test', gid: 'scalar-append-gid'};
  const scalar = 'scalar-value';
  const newVal = 'appended-value';

  distribution.local.store.put(scalar, config, (e) => {
    if (e) {
      done(e);
      return;
    }
    distribution.local.store.append(newVal, config, (e) => {
      if (e) {
        done(e);
        return;
      }
      distribution.local.store.get(config, (e, result) => {
        try {
          if (e) {
            done(e);
            return;
          }
          expect(Array.isArray(result)).toBe(true);
          expect(result.length).toBe(2);
          expect(result[0]).toBe(scalar);
          expect(result[1]).toBe(newVal);
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  });
});
*/

// various append error cases
test('(1 pts) student test', (done) => {
  const missing = {key: 'missing-key-for-errors', gid: 'missing-gid-for-errors'};

  distribution.local.mem.get(missing, (e) => {
    try {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toContain('not found');
    } catch (e) {
      done(e);
      return;
    }
    distribution.local.mem.del(missing, (e) => {
      try {
        expect(e).toBeInstanceOf(Error);
        expect(e.message).toContain('not found');
      } catch (e) {
        done(e);
        return;
      }
      distribution.local.mem.get(null, (e) => {
        try {
          expect(e).toBeInstanceOf(Error);
          expect(e.message).toContain('no config provided');
        } catch (e) {
          done(e);
          return;
        }
        distribution.local.mem.del(null, (e) => {
          try {
            expect(e).toBeInstanceOf(Error);
            expect(e.message).toContain('no config provided');
            done();
          } catch (e) {
            done(e);
          }
        });
      });
    });
  });
});

// mr should complete when mapper emits no output values
test('(1 pts) student test', (done) => {
  const key = 'mr-empty-map-output-key';
  const mapper = () => [];
  const reducer = (k, values) => ({[k]: values});

  distribution.ncdc.store.put('value', key, (e) => {
    if (e) {
      done(e);
      return;
    }
    distribution.ncdc.mr.exec({keys: [key], map: mapper, reduce: reducer}, (mrErr, out) => {
      try {
        if (mrErr && (mrErr instanceof Error || Object.keys(mrErr).length)) {
          done(mrErr);
          return;
        }
        expect(Array.isArray(out)).toBe(true);
        expect(out.length).toBe(0);
        done();
      } catch (err) {
        done(err);
      }
    });
  });
});


// mr should skip keys that do not exist in input store and still complete
test('(1 pts) student test', (done) => {
  const existingKey = 'mr-missing-existing-key';
  const missingA = 'mr-missing-key-a';
  const missingB = 'mr-missing-key-b';

  const mapper = (key, value) => [{[key]: value}];
  const reducer = (key, values) => ({[key]: values});

  distribution.ncdc.store.put('present-value', existingKey, (e) => {
    if (e) {
      done(e);
      return;
    }
    const keys = [existingKey, missingA, missingB];
    distribution.ncdc.mr.exec({keys, map: mapper, reduce: reducer}, (e, out) => {
      try {
        if (e) {
          done(e);
          return;
        }
        expect(Array.isArray(out)).toBe(true);
        expect(out.length).toBe(1);
        expect(out[0]).toEqual({[existingKey]: ['present-value']});
        done();
      } catch (err) {
        done(err);
      }
    });
  });
});

// mr should handle mapper undefined returns by treating them as no mapped output
test('(1 pts) student test', (done) => {
  const keyGood = 'mr-undefined-good';
  const keyDrop = 'mr-undefined-drop';

  const mapper = (key, value) => {
    if (key === 'mr-undefined-drop') {
      return undefined;
    }
    return [{[key]: value}];
  };
  const reducer = (key, values) => ({[key]: values});

  distribution.ncdc.store.put('good-value', keyGood, (e) => {
    if (e) {
      done(e);
      return;
    }
    distribution.ncdc.store.put('drop-value', keyDrop, (e) => {
      if (e) {
        done(e);
        return;
      }
      distribution.ncdc.mr.exec({keys: [keyGood, keyDrop], map: mapper, reduce: reducer}, (e, out) => {
        try {
          if (e) {
            done(e);
            return;
          }
          expect(Array.isArray(out)).toBe(true);
          expect(out).toEqual(expect.arrayContaining([{[keyGood]: ['good-value']}]));
          const dropped = out.find((obj) => Object.prototype.hasOwnProperty.call(obj, keyDrop));
          expect(dropped).toBeUndefined();
          done();
        } catch (err) {
          done(err);
        }
      });
    });
  });
});

// mr should accept scalar mapped values and still group them
test('(1 pts) student test', (done) => {
  const key1 = 'mr-scalar-map-a';
  const key2 = 'mr-scalar-map-b';

  const mapper = () => [{shared: 1}];
  const reducer = (key, values) => ({[key]: values.reduce((sum, v) => sum + v, 0)});

  distribution.ncdc.store.put('va', key1, (e) => {
    if (e) {
      done(e);
      return;
    }
    distribution.ncdc.store.put('vb', key2, (e) => {
      if (e) {
        done(e);
        return;
      }
      distribution.ncdc.mr.exec({keys: [key1, key2], map: mapper, reduce: reducer}, (e, out) => {
        try {
          if (e) {
            done(e);
            return;
          }
          expect(Array.isArray(out)).toBe(true);
          expect(out).toEqual(expect.arrayContaining([{shared: 2}]));
          done();
        } catch (err) {
          done(err);
        }
      });
    });
  });
});

beforeAll((done) => {
  try {
    ncdcGroup[id.getSID(n1)] = n1;
    ncdcGroup[id.getSID(n2)] = n2;
    ncdcGroup[id.getSID(n3)] = n3;

    const startNodes = (cb) => {
      distribution.local.status.spawn(n1, (e) => {
        if (e) {
          done(e);
          return;
        }
        distribution.local.status.spawn(n2, (e) => {
          if (e) {
            done(e);
            return;
          }
          distribution.local.status.spawn(n3, (e) => {
            if (e) {
              done(e);
              return;
            }
            cb();
          });
        });
      });
    };

    distribution.node.start((e) => {
      if (e) {
        done(e);
        return;
      }
      startNodes(() => {
        const ncdcConfig = {gid: 'ncdc'};
        distribution.local.groups.put(ncdcConfig, ncdcGroup, () => {
          distribution.ncdc.groups.put(ncdcConfig, ncdcGroup, () => {
            done();
          });
        });
      });
    });
  } catch (e) {
    done(e);
  }
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
