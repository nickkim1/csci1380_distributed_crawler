/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
require('../helpers/sync-guard');
const {performance} = require('perf_hooks');
const util = distribution.util;

// negative float
test('(1 pts) student test', () => {
  const number = -1.5;
  const serialized = util.serialize(number);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(number);
});

// empty string
test('extra test', () => {
  const string = '';
  const serialized = util.serialize(string);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(string);
});

// unicode string
test('(1 pts) student test', () => {
  const string = '😳';
  const serialized = util.serialize(string);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(string);
});

// can serialize and deserialize with deserialized serialize and deserialize
// (can handle complex named functions)
test('(1 pts) student test', () => {
  const serializedSerialize = util.serialize(util.serialize);
  const serializedDeserialize = util.serialize(util.deserialize);
  const deserializedSerialize = util.deserialize(serializedSerialize);
  const deserializedDeserialize = util.deserialize(serializedDeserialize);

  const object = {a: 1, b: 2, c: [1, 2, 3]};
  const serialized = deserializedSerialize(object);
  const deserialized = deserializedDeserialize(serialized);
  expect(deserialized).toEqual(object);
});

// object w/ same fields as Error
test('(1 pts) student test', () => {
  const object = {name: 'err', message: 'msg', stack: 'stk'};
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(object);
});

// serialize rejects unimplemented types
test('(1 pts) student test', () => {
  expect(() => util.serialize(Symbol())).toThrow();
});

// serialize rejects unimplemented instances of object
test('extra test', () => {
  expect(() => util.serialize(RegExp('.*'))).toThrow();
});

test('performance characterization', () => {
  const latencies = [];

  const runBenchmark = (data) => {
    const start = performance.now();

    const serialized = util.serialize(data);
    util.deserialize(serialized);

    latencies.push(performance.now() - start);
  };

  // base type (numbers)
  for (let i = 0; i < 100; i++) runBenchmark(i);

  // functions
  for (let i = 0; i < 100; i++) runBenchmark((i) => i + i);

  // recursive structures
  const object = {};
  for (let i = 0; i < 100; i++) object[i] = i;
  for (let i = 0; i < 100; i++) runBenchmark(object);

  const latencyTotal = latencies.reduce((a, b) => a + b, 0);
  console.log(`average latency: ${(latencyTotal / latencies.length).toFixed(4)} ms`);
  console.log(`average throughput: ${(latencies.length / latencyTotal).toFixed(4)} msgs/ms`);
});
