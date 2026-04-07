/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
const id = distribution.util.id;
require('../helpers/sync-guard');

// local mem/store get with null key
test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbmpg';

  distribution.local.mem.put(user, key, (e, v) => {
    distribution.local.mem.get(null, (e, v) => {
      try {
        expect(e).toBeInstanceOf(Error);
        expect(v).toBeFalsy();
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

// store.put is idempotent
test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbmpg';

  distribution.local.store.put(user, key, (e, v) => {
    distribution.local.store.put(user, key, (e, v) => {
      try {
        expect(e).toBeFalsy();
      } catch (error) {
        done(error);
      }
      distribution.local.store.get(key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(user);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });
});


// local mem/store differentiates between groups
test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbmpg';

  distribution.local.mem.put(user, key, (e, v) => {
    const groupKey = {key: 'jcarbmpg', gid: 'mygroup'};
    distribution.local.mem.get(groupKey, (e, v) => {
      try {
        expect(e).toBeInstanceOf(Error);
        expect(v).toBeFalsy();
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});


// local mem/store allows identical keys between groups
test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const group1Key = {key: 'jcarbmpg', gid: 'mygroup1'};

  distribution.local.store.put(user, group1Key, (e, v) => {
    const group2Key = {key: 'jcarbmpg', gid: 'mygroup2'};
    distribution.local.store.put(user, group2Key, (e, v) => {
      distribution.local.store.get(group1Key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(user);
        } catch (error) {
          done(error);
        }
        distribution.local.store.get(group2Key, (e, v) => {
          try {
            expect(e).toBeFalsy();
            expect(v).toEqual(user);
            done();
          } catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

// consistent hash wraps around
test('(1 pts) student test', (done) => {
  const key = 'jcarb';
  const nodes = [
    {ip: '127.0.0.1', port: 10000},
    {ip: '127.0.0.1', port: 10001},
    {ip: '127.0.0.1', port: 10002},
  ];

  const kid = id.getID(key);
  const nids = nodes.map((node) => id.getNID(node));

  const hash = id.consistentHash(kid, nids);
  const expectedHash = '8970c41015d3ccbf1f46691ae77ab225aa6c3d401f6c1c5297f4df7ec35c72b0';

  try {
    expect(expectedHash).toBeTruthy();
    expect(hash).toEqual(expectedHash);
    done();
  } catch (error) {
    done(error);
  }
});
