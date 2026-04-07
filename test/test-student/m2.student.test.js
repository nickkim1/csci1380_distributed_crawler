/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
const local = distribution.local;
// const id = distribution.util.id;
// const config = distribution.node.config;
require('../helpers/sync-guard');
const {performance} = require('perf_hooks');

// status.get doesn't panic on empty string
test('(1 pts) student test', (done) => {
  local.status.get('', (e, v) => {
    try {
      expect(e).toBeDefined();
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      done();
    } catch (error) {
      done(error);
    }
  });
});


// routes.get fails gracefully without continuation
test('(1 pts) student test', (done) => {
  try {
    local.routes.get('random');
    done();
  } catch (error) {
    done(error);
  }
});


// routes.put with same key overrides
test('(1 pts) student test', (done) => {
  const echoService = {};
  echoService.echo = () => {
    return 'echo!';
  };

  const otherService = {};
  otherService.gotcha = () => {
    return 'gotcha!';
  };

  local.routes.put(echoService, 'other', (e, v) => {
    local.routes.put(otherService, 'other', (e, v) => {
      local.routes.get('other', (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v.gotcha()).toEqual('gotcha!');
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });
});

// routes.rem handles nonexistent entry gracefully
test('(1 pts) student test', (done) => {
  local.routes.rem('temp', (e, v) => {
    local.routes.get('temp', (e, v) => {
      try {
        expect(e).toBeTruthy();
        expect(e).toBeInstanceOf(Error);
        expect(v).toBeFalsy();
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

// comm.send increments counts
test('(1 pts) student test', (done) => {
  const node = distribution.node.config;
  const remote = {node: node, service: 'status', method: 'get'};
  const message = ['counts'];

  local.comm.send(message, remote, (e, v) => {
    local.status.get('counts', (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toBe(1);
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

// measure latency recursively to get around async
test('characterize comm', ((done) => {
  const node = distribution.node.config;
  const remote = {node: node, service: 'status', method: 'get'};
  const message = ['counts'];

  const n = 1000;
  const latencies = [];

  function measureLatency(count) {
    if (count === 0) {
      const total = latencies.reduce((a, b) => a + b, 0);
      console.log(`average latency: ${(total / latencies.length).toFixed(4)} ms`);
      measureThroughput();
      return;
    }

    const start = performance.now();
    distribution.local.comm.send(message, remote, (e, v) => {
      const duration = performance.now() - start;
      latencies.push(duration);
      measureLatency(count - 1);
    });
  };

  function measureThroughput() {
    let completed = 0;
    const start = performance.now();
    for (let i = 0; i < n; i++) {
      local.comm.send(message, remote, (e, v) => {
        completed++;
        if (completed === n) {
          const totalTime = performance.now() - start;
          console.log(`average throughput: ${(n / totalTime).toFixed(4)} requests/ms`);
          return done();
        }
      });
    }
  }

  measureLatency(n);
}), 10000);

/* Test infrastructure */

beforeAll((done) => {
  distribution.node.start((e) => {
    if (e) {
      done(e);
      return;
    }
    done();
  });
});

afterAll((done) => {
  if (globalThis.distribution.node.server) {
    globalThis.distribution.node.server.close();
  }
  done();
});

