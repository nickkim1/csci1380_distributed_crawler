// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../types.js").Node} Node
 */
const {setup} = require('../all/all.js');
const distribution = globalThis.distribution;
const id = distribution.util.id;

const localGroups = {};

const localNode = distribution.node.config;
localGroups.local = {[id.getSID(localNode)]: localNode};
localGroups.all = localGroups.local;

/**
 * @param {string} name
 * @param {Callback} callback
 */
function get(name, callback) {
  callback = callback || ((e, v) => console.log('no callback provided'));
  let ret = null;
  let err = null;
  if (name in localGroups) {
    ret = localGroups[name];
  } else {
    err = Error(`group ${name} not found`);
  }
  return callback(err, ret);
}

/**
 * @param {Config | string} config
 * @param {Object.<string, Node>} group
 * @param {Callback} callback
 */
function put(config, group, callback) {
  callback = callback || ((e, v) => console.log('no callback provided'));
  config = typeof config === 'string' ? {gid: config} : config;
  const gid = config.gid;
  localGroups[gid] = group;
  Object.assign(localGroups.all, group);
  distribution[gid] = {};
  Object.assign(distribution[gid], setup(config));
  return callback(null, group);
}

/**
 * @param {string} name
 * @param {Callback} callback
 */
function del(name, callback) {
  callback = callback || ((e, v) => console.log('no callback provided'));
  let ret = null;
  let err = null;
  if (name in localGroups) {
    ret = localGroups[name];
    delete localGroups[name];
    delete distribution[name];
  } else {
    err = Error(`service ${name} not found`);
  }
  return callback(err, ret);
}

/**
 * @param {string} name
 * @param {Node} node
 * @param {Callback} callback
 */
function add(name, node, callback) {
  callback = callback || ((e, v) => console.log('no callback provided'));
  let ret = null;
  let err = null;
  if (name in localGroups) {
    localGroups[name][id.getSID(node)] = node;
    localGroups.all[id.getSID(node)] = node;
    ret = localGroups[name];
  } else {
    err = Error(`group ${name} not found`);
  }
  return callback(err, ret);
};

/**
 * @param {string} name
 * @param {string} node
 * @param {Callback} callback
 */
function rem(name, node, callback) {
  callback = callback || ((e, v) => console.log('no callback provided'));
  let ret = null;
  let err = null;
  if (name in localGroups) {
    if (node in localGroups[name]) {
      ret = localGroups[name][node];
      delete localGroups[name][node];
    } else {
      err = Error(`node ${node} not found in group ${name}`);
    }
  } else {
    err = Error(`group ${name} not found`);
  }
  return callback(err, ret);
};

module.exports = {get, put, del, add, rem};
